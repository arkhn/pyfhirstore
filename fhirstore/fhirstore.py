import sys
import re
import logging
import elasticsearch

from collections import defaultdict
from werkzeug.datastructures import ImmutableMultiDict
from pymongo import MongoClient, ReturnDocument, ASCENDING
from pymongo.errors import WriteError, OperationFailure, DuplicateKeyError
from tqdm import tqdm
from jsonschema import validate

from fhirstore import ARKHN_CODE_SYSTEMS
from fhirstore.schema import SchemaParser
from fhirstore.search import SearchArguments, build_core_query, Bundle


class NotFoundError(Exception):
    """
    NotFoundError is returned when a resource was not found in the database.
    """

    pass


class BadRequestError(Exception):
    """
    BadRequestError is returned when the client request could not be processed.
    """

    pass


class FHIRStore:
    def __init__(
        self,
        mongo_client: MongoClient,
        es_client: elasticsearch.Elasticsearch,
        db_name: str,
        resources: dict = {},
    ):
        self.es = es_client
        self.db = mongo_client[db_name]
        self.parser = SchemaParser()
        self.resources = resources

    def reset(self):
        """
        Drops all collections currently in the database.
        """
        for collection in self.db.list_collection_names():
            self.db.drop_collection(collection)
        self.resources = {}

    def bootstrap(self, depth=3, resource=None, show_progress=True):
        """
        Parses the FHIR json-schema and create the collections according to it.
        """
        resources = self.parser.parse(depth=depth, resource=resource)
        if show_progress:
            tqdm.write("\n", end="")
            resources = tqdm(resources, file=sys.stdout, desc="Bootstrapping collections...")
        for resource_name, schema in resources:
            self.db.create_collection(resource_name, **{"validator": {"$jsonSchema": schema}})
            # Add unique constraint on id
            self.db[resource_name].create_index("id", unique=True)
            # Add unique constraint on (identifier.system, identifier.value)
            self.db[resource_name].create_index(
                [
                    ("identifier.system", ASCENDING),
                    ("identifier.value", ASCENDING),
                    ("identifier.type.coding.0.system", ASCENDING),
                    ("identifier.type.coding.0.code", ASCENDING),
                ],
                unique=True,
                partialFilterExpression={"identifier": {"$exists": True}},
            )
            self.resources[resource_name] = schema

    def resume(self, show_progress=True):
        """
        Loads the existing resources schema from the database.
        """
        collections = self.db.list_collection_names()

        if show_progress:
            tqdm.write("\n", end="")
            collections = tqdm(
                collections, file=sys.stdout, desc="Loading collections from database...",
            )

        for collection in collections:
            json_schema = self.db.get_collection(collection).options()["validator"]["$jsonSchema"]
            self.resources[collection] = json_schema

    def validate_resource_type(self, resource_type):
        if resource_type is None:
            raise BadRequestError("resourceType is missing in resource")

        elif resource_type not in self.resources:
            raise NotFoundError(f'unsupported FHIR resource: "{resource_type}"')

    def create(self, resource, bypass_document_validation=False):
        """
        Creates a resource. The structure of the resource will be checked
        against its json-schema FHIR definition.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').

        Returns: The updated resource.
        """
        resource_type = resource.get("resourceType")
        self.validate_resource_type(resource_type)

        try:
            res = self.db[resource_type].insert_one(
                resource, bypass_document_validation=bypass_document_validation
            )
            return {**resource, "_id": res.inserted_id}
        except DuplicateKeyError as e:
            raise e
        except WriteError:
            self.validate(resource)

    def read(self, resource_type, instance_id):
        """
        Finds a resource given its type and id.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').

        Returns: The found resource.
        """
        self.validate_resource_type(resource_type)

        res = self.db[resource_type].find_one({"id": instance_id})
        if res is None:
            raise NotFoundError
        return res

    def update(self, resource_type, instance_id, resource, bypass_document_validation=False):
        """
        Update a resource given its type, id and a resource. It applies
        a "replace" operation, therefore the resource will be overriden.
        The structure of the updated resource will  be checked against
        its json-schema FHIR definition.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').
            - resource: The updated resource.

        Returns: The updated resource.
        """
        self.validate_resource_type(resource_type)

        try:
            update_result = self.db[resource_type].replace_one(
                {"id": instance_id},
                resource,
                bypass_document_validation=bypass_document_validation,
            )
            if update_result.matched_count == 0:
                raise NotFoundError
            return update_result
        except OperationFailure:
            self.validate(resource)

    def patch(self, resource_type, instance_id, patch, bypass_document_validation=False):
        """
        Update a resource given its type, id and a patch. It applies
        a "patch" operation rather than a "replace", only the fields
        specified in the third argument will be updated. The structure
        of the updated resource will  be checked against its json-schema
        FHIR definition.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').
            - patch: The patch to be applied on the resource.

        Returns: The updated resource.
        """
        self.validate_resource_type(resource_type)

        try:
            update_result = self.db[resource_type].update_one(
                {"id": instance_id},
                {"$set": patch},
                bypass_document_validation=bypass_document_validation,
            )
            if update_result.matched_count == 0:
                raise NotFoundError
            return update_result
        except OperationFailure:
            resource = self.read(resource_type, instance_id)
            self.validate({**resource, **patch})

    def delete(self, resource_type, instance_id=None, resource_id=None, source_id=None):
        """
        Deletes a resource given its type and id.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').

        Returns: The id of the deleted resource.
        """
        self.validate_resource_type(resource_type)

        if instance_id:
            res = self.db[resource_type].delete_one({"id": instance_id})
        elif resource_id:
            res = self.db[resource_type].delete_many(
                {
                    "meta.tag": {
                        "$elemMatch": {
                            "code": {"$eq": resource_id},
                            "system": {"$eq": ARKHN_CODE_SYSTEMS.resource},
                        }
                    }
                }
            )
        elif source_id:
            res = self.db[resource_type].delete_many(
                {
                    "meta.tag": {
                        "$elemMatch": {
                            "code": {"$eq": source_id},
                            "system": {"$eq": ARKHN_CODE_SYSTEMS.source},
                        }
                    }
                }
            )
        else:
            raise BadRequestError(
                "one of: 'instance_id', 'resource_id' or 'source_id' are required"
            )

        if res.deleted_count == 0:
            raise NotFoundError

        return res.deleted_count

    def validate(self, resource):
        """
        Validates the given resource against its own schema.
        This is much more efficient than running th validation against the
        whole FHIR json schema.
        This function is useful because MongoDB does not provide any feedback
        about the schema validation error other than "Schema validation failed"

        Args:
            resource: The object to be validated against the schema.
                      It is expected to have the "resourceType" property.
        """
        schema = self.resources.get(resource["resourceType"])
        if schema is None:
            raise Exception(f"missing schema for resource {resource}")
        validate(instance=resource, schema=schema)

    def search(self, search_args: SearchArguments):
        """
        Searchs for params inside a resource.
        Returns a bundle of items, as required by FHIR standards.

        Args:
            - resource_type: FHIR resource (eg: 'Patient')
            - params: search parameters as returned by the API. For a simple
            search, the parameters should be of the type {"key": "value"}
            eg: {"gender":"female"}, with possible modifiers {"address.city:exact":"Paris"}.
            If a search is made one field with multiple arguments (eg: language is French 
            OR English), params should be a payload of type {"multiple": {"language": 
            ["French", "English"]}}. 
            If a search has more than one field queried, params should be a payload of 
            the form: {"address.city": ["Paris"], "multiple":
            {"language": ["French", "English"]}}.
        Returns: A bundle with the results of the search, as required by FHIR
        search standard.
        """
        self.validate_resource_type(search_args.resource_type)

        core_query = build_core_query(search_args.core_args)
        bundle = Bundle()

        if search_args.formatting_args["is_summary_count"]:
            hits = self.es.count(
                body={"query": core_query}, index=f"fhirstore.{search_args.resource_type.lower()}",
            )
            bundle.fill(hits, search_args.formatting_args)

        else:
            query = {
                "min_score": 0.01,
                "from": search_args.meta_args["offset"],
                "size": search_args.meta_args["result_size"],
                "query": core_query,
            }

            if search_args.formatting_args["sort"]:
                query["sort"] = search_args.formatting_args["sort"]

            if search_args.formatting_args["elements"]:
                query["_source"] = search_args.formatting_args["elements"]

            # .lower() is used to fix the fact that monstache changes resourceTypes to
            # all lower case
            hits = self.es.search(
                body=query, index=f"fhirstore.{search_args.resource_type.lower()}"
            )

            bundle.fill(hits, search_args.formatting_args)

        return bundle

    def comprehensive_search(self, resource_type: str, args: ImmutableMultiDict):
        """ To deal with keywords : _include, _revinclude, _has
        """
        search_args = SearchArguments()
        search_args.parse(args, resource_type)

        # handle _has
        rev_chain = search_args.reverse_chain
        if rev_chain and rev_chain.is_queried:
            inner_ids = []
            # If there is a double _has chain
            if len(rev_chain.has_args) == 2:
                outer_ids = []
                rev_args_outer = SearchArguments()

                # parse the outer chain and search the store
                rev_args_outer.parse(rev_chain.has_args[1], rev_chain.resources_type[1])
                chained_bundle_outer = self.search(rev_args_outer)

                for item in chained_bundle_outer.content["entry"]:
                    outer_ids.append(
                        item["resource"][rev_chain.references[1]]["reference"].split(
                            sep="/", maxsplit=1
                        )[1]
                    )

                # fill the inner chain with the ids from the previous search
                rev_chain.has_args[0][rev_chain.fields[0]] = outer_ids

            # If there is a single _has chain, fill it with the value
            elif len(rev_chain.has_args) == 1:
                rev_chain.has_args[0][rev_chain.fields[0]] = rev_chain.value

            rev_args_inner = SearchArguments()
            rev_args_inner.parse(rev_chain.has_args[0], rev_chain.resources_type[0])
            chained_bundle_inner = self.search(rev_args_inner)

            for item in chained_bundle_inner.content["entry"]:
                inner_ids.append(
                    item["resource"][rev_chain.references[0]]["reference"].split(
                        sep="/", maxsplit=1
                    )[1]
                )
            search_args.core_args["id"] = inner_ids
            if search_args.core_args["id"] == []:
                bundle = Bundle()
                bundle.fill_error(
                    severity="warning",
                    code="not-found",
                    details=f"No {rev_chain.resources_type[0]} matching search criteria",
                )
                return bundle

        bundle = self.search(search_args)

        ## handle _include
        if (
            search_args.formatting_args["include"]
            and not search_args.formatting_args["is_summary_count"]
        ):
            included_hits = {}
            for item in bundle.content["entry"]:
                # only go over items that are not the result of an inclusion
                if item["search"]["mode"] == "include":
                    continue
                # For each attribute to include
                for attribute in search_args.formatting_args["include"]:
                    # split the reference attribute "Practioner/123" into a
                    # resource "Practioner" and an id "123"
                    try:
                        included_resource, included_id = item["resource"][attribute][
                            "reference"
                        ].split(sep="/", maxsplit=1)
                        included_hits = self.es.search(
                            body={
                                "query": {
                                    "simple_query_string": {"query": included_id, "fields": ["id"],}
                                }
                            },
                            index=f"fhirstore.{included_resource.lower()}",
                        )
                    except KeyError as e:
                        logging.warning(f"Attribute: {e} is empty")
                    except elasticsearch.exceptions.NotFoundError as e:
                        logging.warning(
                            f"{e.info['error']['index']} is not indexed in the database yet."
                        )

            bundle.append(included_hits, search_args.formatting_args)

        return bundle

    def upload_bundle(self, bundle):
        """
        Upload a bundle of resource instances to the store.

        Args:
            - bundle: the fhir bundle containing the resources.
        """
        if not "resourceType" in bundle or bundle["resourceType"] != "Bundle":
            raise Exception("input must be a FHIR Bundle resource")

        for entry in bundle["entry"]:
            if "resource" not in entry:
                raise Exception("Bundle entry is missing a resource.")

            try:
                self.create(entry["resource"])
            except DuplicateKeyError as e:
                logging.warning(f"Document already existed: {e}")
