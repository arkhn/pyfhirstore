import os
import json
from copy import deepcopy

resource_blacklist = ['Parameters']


def clean_resource(resource):
    if "extension" in resource["properties"]:
        del resource["properties"]["extension"]
    if "modifierExtension" in resource["properties"]:
        del resource["properties"]["modifierExtension"]
    if "contained" in resource["properties"]:
        del resource["properties"]["contained"]


def compatibilize_schema(resource):
    if "const" in resource:
        resource["enum"] = [resource["const"]]
        del resource["const"]
    return resource


def enforce_id(schema):
    schema["properties"]["_id"] = {
        "bsonType": "objectId"
    }
    return schema


class SchemaParser:
    def __init__(self, path: str):
        schema_path = os.path.join(os.getcwd(), path)
        with open(schema_path, 'r') as schemaFile:
            self.schema = json.load(schemaFile)
            self.definitions = self.schema["definitions"]
            self.resources = {
                r["$ref"]: os.path.basename(r["$ref"])
                for r in self.schema["oneOf"]
                if os.path.basename(r["$ref"]) not in resource_blacklist
            }

    def parse(self, resource=None, depth=3):
        if resource is None:
            for ref, resource in self.resources.items():
                yield (resource, self.parse_schema(resource, depth))
        else:
            yield (resource, self.parse_schema(resource, depth))

    def parse_schema(self, resource, depth):
        print(f"Parsing schema of {resource}...")
        if resource in resource_blacklist:
            raise Exception(
                f"Resource {resource} is blacklisted! Aborting the mission.")

        r = self.definitions[resource]
        dereferenced_schema = self.resolve(r, depth)
        dereferenced_schema = enforce_id(dereferenced_schema)
        with open(f'schema/resources/{resource}.schema.json', 'w') as out:
            json.dump(dereferenced_schema, out, indent=2)
        return dereferenced_schema

    def resolve(self, resource, depth):
        if depth == 0:
            if isinstance(resource, dict) and "properties" in resource:
                for k in list(resource["properties"].keys()):
                    resource["properties"][k] = compatibilize_schema(
                        resource["properties"][k])
                    if "$ref" in resource["properties"][k]:
                        del resource["properties"][k]
                    elif "items" in resource["properties"][k]\
                            and "$ref" in resource["properties"][k]["items"]:
                        del resource["properties"][k]
            elif isinstance(resource, dict) and "oneOf" in resource:
                return None

            resource = compatibilize_schema(resource)
            return resource

        if "$ref" in resource:
            path = os.path.basename(resource["$ref"])
            return self.resolve(deepcopy(self.definitions[path]), depth-1)

        if isinstance(resource, dict) and "properties" in resource:
            clean_resource(resource)
            for p in list(resource["properties"].keys()):
                resolved = self.resolve(resource["properties"][p], depth)
                if resolved:
                    resource["properties"][p] = resolved
                else:
                    del resource["properties"][p]

        if isinstance(resource, dict) and "items" in resource:
            resolved = self.resolve(resource["items"], depth)
            if resolved:
                resource["items"] = resolved
            else:
                return None

        if isinstance(resource, dict) and "oneOf" in resource:
            for i, value in enumerate(resource["oneOf"]):
                resource["oneOf"][i] = self.resolve(value, depth)

        resource = compatibilize_schema(resource)
        return resource
