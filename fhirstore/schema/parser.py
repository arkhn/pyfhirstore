import os
import json
from copy import deepcopy

from jsonschema import validate

# resource_blacklist contains resources that should not be taken into account
# while parsing the whole schema
resource_blacklist = [
    # resource 'Parameters' contains all other resources
    # and is too big (144Mo with depth=5). As the 'Parameters' resource is
    # only used at the API level, it should not be persisted in storage and is
    # therefore blacklisted.
    "Parameters"
]


def clean_resource(resource):
    """
    Removes unwanted fields from the resource properties.
    As the following properties result in a way too big schema,
    we decided to ignore them for now:
         - extension
         - modifierExtension
         - contained
    """
    if "extension" in resource["properties"]:
        del resource["properties"]["extension"]
    if "modifierExtension" in resource["properties"]:
        del resource["properties"]["modifierExtension"]
    if "contained" in resource["properties"]:
        del resource["properties"]["contained"]


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
        schema_path = os.path.join(
            os.path.dirname(__file__), "fhir.schema.json")
        with open(schema_path, "r") as schemaFile:
            self.schema = json.load(schemaFile)
            self.definitions = self.schema["definitions"]
            self.resources = {
                os.path.basename(r["$ref"]): None
                for r in self.schema["oneOf"]
                if os.path.basename(r["$ref"]) not in resource_blacklist
            }

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
            for resource in self.resources.keys():
                self.resources[resource] = self.parse_schema(resource, depth)
                yield (resource, self.resources[resource])
        else:
            self.resources[resource] = self.parse_schema(resource, depth)
            yield (resource, self.resources[resource])

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
        if resource in resource_blacklist:
            raise Exception(
                f"Resource {resource} is blacklisted! Aborting the mission."
            )

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
        if depth == 0 and "$ref" in resource:
            return None

        # resource is a reference
        # resolve its definition
        # (NB: deepcopy is required, resource will be modified in place)
        elif "$ref" in resource:
            path = os.path.basename(resource["$ref"])
            return self.resolve(deepcopy(self.definitions[path]), depth - 1)

        # resource is an object
        # resolve each of its properties recursively
        elif "properties" in resource:
            clean_resource(resource)
            for k in list(resource["properties"].keys()):
                resolved = self.resolve(resource["properties"][k], depth)
                if resolved:
                    resource["properties"][k] = resolved
                else:
                    del resource["properties"][k]

        # resource is an array
        # resolve its "items" property
        elif "items" in resource:
            resource["items"] = self.resolve(resource["items"], depth)
            if resource["items"] is None:
                return None

        # resource is a "oneOf" object
        # resolve each of its "oneOf" properties
        elif "oneOf" in resource:
            resource["oneOf"] = list(
                map(lambda x: self.resolve(x, depth), resource["oneOf"])
            )
            if len(list(filter(lambda x: x, resource["oneOf"]))) == 0:
                return None

        compatibilize_schema(resource)
        return resource

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
            raise Exception("missing schema")
        validate(instance=resource, schema=schema)
