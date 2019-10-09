import sys
import os
import json

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, WriteError
from tqdm import tqdm

from fhirstore.schema import SchemaParser


class DatabaseError(Exception):
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

    def create(self, resource):
        resource_type = resource.get("resourceType")
        if resource_type is None:
            raise BadRequestError("resourceType is missing in resource")

        if resource_type not in self.db.list_collection_names():
            raise BadRequestError(
                f'schema for resource "{resource_type}" is missing in database'
            )

        try:
            res = self.db[resource_type].insert_one(resource)
        except WriteError as err:
            self.parser.validate(resource)

    def read(self, resource):
        pass

    def update(self, resource):
        pass

    def delete(self, resource):
        pass
