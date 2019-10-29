import sys
import os
import json

from pymongo import MongoClient, ReturnDocument
from pymongo.errors import (
    ServerSelectionTimeoutError,
    WriteError,
    OperationFailure,
)
from tqdm import tqdm
from jsonschema import validate

from fhirstore.schema import SchemaParser


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
    def __init__(self, client: MongoClient, db_name: str):
        self.db = client[db_name]
        self.parser = SchemaParser()
        self.resources = {}

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
            resources = tqdm(
                resources,
                leave=False,
                file=sys.stdout,
                desc="Bootstrapping collections...",
            )
        for resource_name, schema in resources:
            ret = self.db.create_collection(
                resource_name, **{"validator": {"$jsonSchema": schema}}
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
                collections,
                leave=False,
                file=sys.stdout,
                desc="Loading collections from database...",
            )

        for collection in collections:
            json_schema = self.db.get_collection(
                collection).options()['validator']['$jsonSchema']
            self.resources[collection] = json_schema

    def validate_resource_type(self, resource_type):
        if resource_type is None:
            raise BadRequestError("resourceType is missing in resource")

        elif resource_type not in self.resources:
            raise NotFoundError(
                f'unsupported FHIR resource: "{resource_type}"'
            )

    def create(self, resource):
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
            res = self.db[resource_type].insert_one(resource)
            return {**resource, "_id": res.inserted_id}
        except WriteError as err:
            self.validate(resource)

    def read(self, resource_type, resource_id):
        """
        Finds a resource given its type and id.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').

        Returns: The found resource.
        """
        self.validate_resource_type(resource_type)

        res = self.db[resource_type].find_one({"id": resource_id})
        if res is None:
            raise NotFoundError
        return res

    def update(self, resource_type, resource_id, resource):
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
                {"id": resource_id},
                resource,
                return_document=ReturnDocument.AFTER,
            )
            if updated is None:
                raise NotFoundError
            return updated
        except OperationFailure as err:
            self.validate(resource)

    def patch(self, resource_type, resource_id, patch):
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
                {"id": resource_id},
                {"$set": patch},
                return_document=ReturnDocument.AFTER,
            )
            if updated is None:
                raise NotFoundError
            return updated
        except OperationFailure as err:
            self.read(resource_type, resource_id)
            self.validate({**self.resource, **patch})

    def delete(self, resource_type, resource_id):
        """
        Deletes a resource given its type and id.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').

        Returns: The id of the deleted resource.
        """
        self.validate_resource_type(resource_type)

        res = self.db[resource_type].delete_one({"id": resource_id})
        if res.deleted_count == 0:
            raise NotFoundError
        return resource_id

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
