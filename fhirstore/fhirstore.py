import sys
import re

from collections import defaultdict
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import WriteError, OperationFailure
from tqdm import tqdm
from jsonschema import validate
from elasticsearch import Elasticsearch

from fhirstore import ARKHN_CODE_SYSTEMS
from fhirstore.schema import SchemaParser
from fhirstore.search.search_methods import build_simple_query, validate_parameters


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
        self, client: MongoClient, client_es: Elasticsearch, db_name: str, resources: dict = {},
    ):
        self.es = client_es
        self.db = client[db_name]
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

    def update(self, resource_type, instance_id, resource):
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
            updated = self.db[resource_type].find_one_and_replace(
                {"id": instance_id}, resource, return_document=ReturnDocument.AFTER
            )
            if updated is None:
                raise NotFoundError
            return updated
        except OperationFailure:
            self.validate(resource)

    def patch(self, resource_type, instance_id, patch):
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
            updated = self.db[resource_type].find_one_and_update(
                {"id": instance_id}, {"$set": patch}, return_document=ReturnDocument.AFTER,
            )
            if updated is None:
                raise NotFoundError
            return updated
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

    def search(self, resource_type, params, offset=0, result_size=100):
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
        validate_parameters(params)

        sub_query = defaultdict(lambda: defaultdict(dict))
        if len(params) == 0:
            sub_query = {"match_all": {}}
        elif len(params) == 1:
            sub_query = build_simple_query(params)
        elif len(params) > 1:
            inter_query = [
                build_simple_query({sub_key: sub_value}) for sub_key, sub_value in params.items()
            ]
            sub_query = {"bool": {"must": inter_query}}
        query = {"min_score": 0.01, "from": offset, "size": result_size, "query": sub_query}

        # .lower() is used to fix the fact that monstache changes resourceTypes to
        # all lower case
        hits = self.es.search(body=query, index=f"fhirstore.{resource_type.lower()}")
        results = [h["_source"] for h in hits["hits"]["hits"]]

        return {"resource_type": "Bundle", "items": results}
