import os
import json
from copy import deepcopy


# Do not resolve the following resources when parsing the schema.
definitions_blacklist = ["ResourceList"]  # ResourceList embeds all other resources,


def compatibilize_schema(resource):
    """
    As MongoDB only supports draft #4 of hte JSON schema standard,
    (https://tools.ietf.org/html/draft-zyp-json-schema-04)
    we have to manually migrate some of the keywords:
        - const
    """
    if "const" in resource:
        resource["enum"] = [resource["const"]]
        del resource["const"]


def enforce_id(schema):
    """
    As MongoDB automatically adds an '_id' property when insertign a document
    in the database, we have to update the schema accordingly.
    """
    schema["properties"]["_id"] = {"bsonType": "objectId"}


class SchemaParser:
    def __init__(self):
        """
        Initialized the parsed using the "fhir.schema.json" file
        located in the same directory.
        """
        schema_path = os.path.join(os.path.dirname(__file__), "fhir.schema.json")
        with open(schema_path, "r") as schemaFile:
            self.schema = json.load(schemaFile)
            self.definitions = self.schema["definitions"]
            self.resources = [os.path.basename(r["$ref"]) for r in self.schema["oneOf"]]

    def parse(self, resource=None, depth=4):
        """
        Parse the whole JSON schema or a single definition if `resource`
        is specified.

        Args:
            resource (optional): name of the resource (eg: 'Patient')
            depth (optional): Number of recursion to apply.

        Returns: a generator that yields each parsed schema as a tuple:
                    (resource_name, schema)
        """
        if resource is None:
            for resource in self.resources:
                schema = self.parse_schema(resource, depth)
                yield (resource, schema)
        else:
            schema = self.parse_schema(resource, depth)
            yield (resource, schema)

    def parse_schema(self, resource: str, depth: int):
        """
        Parses the JSON schema of a single definition.
        It also adds (or overrides) the property '_id' of the resource;
        as required by MongoDB.

        Args:
            resource: The name of the resource (eg: 'Patient')
            depth: Number of recursion to apply.

        Returns: The dereferenced schema.
                 (ready to be passed as mongoDB 'validator')
        """

        r = self.definitions[resource]
        dereferenced_schema = self.resolve(r, depth)
        enforce_id(dereferenced_schema)
        return dereferenced_schema

    def resolve(self, resource: dict, depth: int):
        """
        Dereference a JSON schema recursively.

        Args:
            resource: A single JSON schema definition.
                      Used in the recursions as either:
                        - an internal reference (dict with "$ref" key)
                        - a JSON schema object (dict with "properties" key)
                        - a JSON schema array (dict with "items" key)
                        - a JSON schema "oneOf" object (dict with "oneOf" key)
            depth: The number of internal references to recursively resolve.

        Returns: The dereferenced schema.

        """
        # stop resolving references if maximal depth is reached
        # the reference is removed from the resource but all other
        # properties are kept.
        if depth == 0 and "$ref" in resource:
            del resource["$ref"]
            return resource

        # resource is a reference
        # resolve its definition if not blacklisted.
        # (NB: deepcopy is required, resource will be modified in place)
        elif "$ref" in resource:
            path = os.path.basename(resource["$ref"])
            if path not in definitions_blacklist:
                return self.resolve(deepcopy(self.definitions[path]), depth - 1)
            del resource["$ref"]

        # resource is an object
        # resolve each of its properties recursively
        elif "properties" in resource:
            for k in list(resource["properties"].keys()):
                # only one level of recursion is allowed for extensions
                if k == "extension" or k == "modifierExtension":
                    resource["properties"][k] = self.resolve(
                        resource["properties"][k], depth if depth < 1 else 1
                    )
                else:
                    resource["properties"][k] = self.resolve(resource["properties"][k], depth)

        # resource is an array
        # resolve its "items" property
        elif "items" in resource:
            resource["items"] = self.resolve(resource["items"], depth)

        # resource is a "oneOf" object
        # resolve each of its "oneOf" properties
        elif "oneOf" in resource:
            resource["oneOf"] = [self.resolve(x, depth) for x in resource["oneOf"]]

        compatibilize_schema(resource)
        return resource
