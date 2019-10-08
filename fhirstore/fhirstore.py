import os
import json
from pymongo import MongoClient

from fhirstore.schema import SchemaParser


class FHIRStore():
    def __init__(self, client: MongoClient, db_name: str):
        self.db = client[db_name]

    def reset(self):
        for collection in self.db.list_collection_names():
            self.db.drop_collection(collection)

    def bootstrap(self, schema_path, depth=4):
        parser = SchemaParser(schema_path)
        for resource_name, schema in parser.parse(depth=depth):
            # print(json.dumps(schema, indent=2))
            ret = self.db.create_collection(
                resource_name, **{
                    'validator': {
                        '$jsonSchema': schema
                    }
                }
            )

    def create(self, resource):
        try:
            collection = resource['resourceType']
            self.db[collection].insert_one(resource)
        except Exception as e:
            raise e

    def read(self, resource):
        pass

    def update(self, resource):
        pass

    def delete(self, resource):
        pass
