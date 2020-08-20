import sys
import logging
import elasticsearch

import json
import pydantic

from multidict import MultiDict
from pymongo import MongoClient, ASCENDING
from pymongo.errors import WriteError, OperationFailure, DuplicateKeyError
from tqdm import tqdm

from fhirpath.search import SearchContext, Search

from fhir.resources import construct_fhir_element
from fhir.resources.operationoutcome import OperationOutcome

from fhirstore import ARKHN_CODE_SYSTEMS
from fhirstore.schema import SchemaParser
from fhirstore.search_engine import ElasticSearchEngine, create_search_engine


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
        resources: list = [],
    ):
        self.es = es_client
        self.db = mongo_client[db_name]
        self.parser = SchemaParser()
        self.resources = resources

        if self.es and len(self.es.transport.hosts) > 0:
            self.search_engine: ElasticSearchEngine = create_search_engine(self.es)
        else:
            logging.warning("No elasticsearch client provided, search features are disabled")

    def reset(self):
        """
        Drops all collections currently in the database.
        """
        for collection in self.db.list_collection_names():
            self.db.drop_collection(collection)
        self.es.indices.delete("_all")
        self.resources = []

    def bootstrap(self, resource=None, show_progress=True):
        """
        Parses the FHIR json-schema and create the collections according to it.
        """
        existing_resources = self.db.list_collection_names()

        # Bootstrap elastic indices
        self.search_engine.create_es_index(resource=resource)

        # Bootstrap mongoDB collections
        self.resources = (
            [*self.resources, resource] if resource else self.search_engine.mappings.keys()
        )
        resources = [r for r in self.resources if r not in existing_resources]
        if show_progress:
            tqdm.write("\n", end="")
            resources = tqdm(resources, file=sys.stdout, desc="Bootstrapping collections...",)
        for resource_type in resources:
            self.db.create_collection(resource_type)
            # Add unique constraint on id
            self.db[resource_type].create_index("id", unique=True)
            # Add unique constraint on (identifier.system, identifier.value)
            self.db[resource_type].create_index(
                [
                    ("identifier.system", ASCENDING),
                    ("identifier.value", ASCENDING),
                    ("identifier.type.coding.system", ASCENDING),
                    ("identifier.type.coding.code", ASCENDING),
                ],
                unique=True,
                partialFilterExpression={"identifier": {"$exists": True}},
            )

    def resume(self, show_progress=True):
        """
        Loads the existing resources schema from the database.
        """
        self.resources = self.db.list_collection_names()

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
            - resource: dict with the resource data

        Returns: The created resource.
        """
        try:
            r = construct_fhir_element(resource.resource_type, resource)
            res = self.db[resource.resource_type].insert_one(
                json.loads(r.json()), bypass_document_validation=bypass_document_validation
            )
            return {**r.dict(), "_id": res.inserted_id}
        except DuplicateKeyError as e:
            raise e
        except WriteError:
            return self.validate(resource)

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
            return self.validate(resource)

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
            return self.validate({**resource, **patch})

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
        try:
            construct_fhir_element(resource["resourceType"], resource)
        except pydantic.ValidationError as e:
            issues = []
            for err in e.errors():
                issues.append(
                    {
                        "severity": "error",
                        "code": "invalid",
                        "diagnostics": f"{err['msg']}: "
                        f"{','.join([f'{e.model.get_resource_type()}.{l}' for l in err['loc']])}",
                    }
                )
            return OperationOutcome(**{"issue": issues})

    def search(self, resource_type, query_string=None, params=None):
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
        self.validate_resource_type(resource_type)

        params = (
            Search.parse_query_string(query_string)
            if query_string is not None
            else MultiDict(params.items())
        )

        # eg: ?_has:diagnosticsReport:subject:code.coding.code=
        # pop _has params and parse them
        _has = []
        for key in [k for k in params.keys() if k.startswith("_has:")]:
            value = params.pop(key)
            parts = key.split(":")
            from_resource_type = parts[1]
            ref_attribute = parts[2]
            target_searchparam = parts[3]
            _has.append(
                {
                    "resource_type": from_resource_type,
                    "ref_attribute": ref_attribute,
                    "value": value,
                    "target_searchparam": target_searchparam,
                }
            )

        # pop _include params and parse them
        _include = []
        for i in params.popall("_include", []):
            parts = i.split(":")
            from_resource_type = parts[0]
            ref_attribute = parts[1]
            _include.append({"resource_type": from_resource_type, "ref_attribute": ref_attribute})

        # print("_has", _has)
        # print("_include", _include)
        # print(dict(params))

        # send _has request
        # http://localhost:5000/Encounter?_has:Encounter:serviceProvider:name=test&_count=1
        # for _h in _has:
        # m = get_fhir_model_class(_h["resource_type"])
        # print("model :: ", m)
        # search_context = SearchContext(self.search_engine, _h["resource_type"])
        # fhir_search = Search(
        #     search_context,
        #     query_string=query_string,
        #     params={_h["target_searchparam"]: _h["value"]},
        # )
        # bundle = fhir_search()
        # print("res _has", bundle.as_json())
        # inner_ids = []
        # for e in bundle.entry:
        #     inner_ids.append(e)

        search_context = SearchContext(self.search_engine, resource_type)
        fhir_search = Search(search_context, params=dict(params))
        try:
            return fhir_search()
        except elasticsearch.exceptions.NotFoundError as e:
            return OperationOutcome(
                {
                    "issue": [
                        {
                            "severity": "error",
                            "code": "invalid",
                            "diagnostics": f"{e.info['error']['index']}"
                            "is not indexed in the database yet.",
                        }
                    ]
                }
            )
        except elasticsearch.exceptions.RequestError as e:
            # raise Exception(e.info["error"]["root_cause"])
            return OperationOutcome(
                {
                    "issue": [
                        {
                            "severity": "error",
                            "code": "invalid",
                            "diagnostics": e.info["error"]["root_cause"],
                        }
                    ]
                }
            )
        except elasticsearch.exceptions.AuthenticationException as e:
            return OperationOutcome(
                {
                    "issue": [
                        {
                            "severity": "error",
                            "code": "invalid",
                            "diagnostics": e.info["error"]["root_cause"],
                        }
                    ]
                }
            )
        except pydantic.ValidationError as e:
            issues = []
            for err in e.errors():
                issues.append(
                    {
                        "severity": "error",
                        "code": "invalid",
                        "diagnostics": f"{err['msg']}: "
                        f"{','.join([f'{e.model.get_resource_type()}.{l}' for l in err['loc']])}",
                    }
                )
            return OperationOutcome(**{"issue": issues})

    def upload_bundle(self, bundle):
        """
        Upload a bundle of resource instances to the store.

        Args:
            - bundle: the fhir bundle containing the resources.
        """
        if "resourceType" not in bundle or bundle["resourceType"] != "Bundle":
            raise Exception("input must be a FHIR Bundle resource")

        for entry in bundle["entry"]:
            if "resource" not in entry:
                raise Exception("Bundle entry is missing a resource.")

            try:
                self.create(entry["resource"])
            except DuplicateKeyError as e:
                logging.warning(f"Document already existed: {e}")
