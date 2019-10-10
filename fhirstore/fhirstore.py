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

from fhirstore.schema import SchemaParser


class NotFoundError(Exception):
    pass


class BadRequestError(Exception):
    pass


class FHIRStore:
    def __init__(self, client: MongoClient, db_name: str):
        self.db = client[db_name]
        self.parser = SchemaParser()

    def reset(self):
        """
        Drops all collections currently in the database.
        """
        for collection in self.db.list_collection_names():
            self.db.drop_collection(collection)

    def bootstrap(self, depth=4, resource=None, show_progress=True):
        """
        Parses the FHIR json-schema and create the collections according to it.
        """
        resources = self.parser.parse(depth=depth, resource=resource)
        if show_progress:
            tqdm.write("\n", end="")
            resources = tqdm(
                resources,
                total=len(self.parser.resources),
                leave=False,
                file=sys.stdout,
                desc="Bootstrapping collections...",
            )
        for resource_name, schema in resources:
            ret = self.db.create_collection(
                resource_name, **{"validator": {"$jsonSchema": schema}}
            )

    def validate_resource_type(self, resource_type):
        if resource_type is None:
            raise BadRequestError("resourceType is missing in resource")

        elif resource_type not in self.db.list_collection_names():
            raise BadRequestError(
                f'schema for resource "{resource_type}" is missing in database'
            )

    def create(self, resource):
        resource_type = resource.get("resourceType")
        self.validate_resource_type(resource_type)

        try:
            res = self.db[resource_type].insert_one(resource)
            return res.inserted_id
        except WriteError as err:
            self.parser.validate(resource)

    def read(self, resource_type, resource_id):
        self.validate_resource_type(resource_type)

        res = self.db[resource_type].find_one({"id": resource_id})
        if res is None:
            raise NotFoundError
        return res

    def update(self, resource_type, resource_id, patch):
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
            self.parser.validate({**patch, "resourceType": resource_type})

    def delete(self, resource_type, resource_id):
        self.validate_resource_type(resource_type)

        res = self.db[resource_type].delete_one({"id": resource_id})
        if res.deleted_count == 0:
            raise NotFoundError
        return resource_id
