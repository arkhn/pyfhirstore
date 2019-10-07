import os
import json
from pymongo import MongoClient

from fhirstore.schema import SchemaParser


class FHIRStore:
    def __init__(self, client: MongoClient, db_name: str):
        self.db = client[db_name]

    def reset(self):
        """
        Drops all collections currently in the database.
        """
        for collection in self.db.list_collection_names():
            self.db.drop_collection(collection)

    def bootstrap(self, depth=4):
        """
        Parses the FHIR json-schema and create the collections according to it.
        """
        parser = SchemaParser()
        for resource_name, schema in parser.parse(depth=depth):
            ret = self.db.create_collection(
                resource_name, **{"validator": {"$jsonSchema": schema}}
            )

    def create(self, resource):
        collection = resource["resourceType"]
        self.db[collection].insert_one(resource)

    def read(self, resource):
        pass

    def update(self, resource):
        pass

    def delete(self, resource):
        pass
