import json
from pytest import raises

from pymongo import MongoClient
from fhirstore import FHIRStore, BadRequestError
from jsonschema.exceptions import ValidationError

# For now, this class assumes an already existing store exists
# (store.bootstrap was run)


class TestFHIRStore:
    "FHIRStore"

    ###
    # FHIRStore.create()
    ###
    def test_create_missing_resource_type(
        self, store: FHIRStore, mongo_client: MongoClient
    ):
        """create() raises if resource type is not specified"""

        with raises(
            BadRequestError, match="resourceType is missing in resource"
        ):
            store.create({})

    def test_create_bad_resource_type(
        self, store: FHIRStore, mongo_client: MongoClient
    ):
        """create() raises if resource type is unknown"""

        with raises(
            BadRequestError,
            match='schema for resource "unknown" is missing in database',
        ):
            store.create({"resourceType": "unknown"})

    def test_create_bad_resource_schema(
        self, store: FHIRStore, mongo_client: MongoClient
    ):
        """create() raises if json schema validation failed in mongo"""

        with raises(ValidationError):
            store.create({"resourceType": "Patient", "id": 42})

    def test_create_resource(
        self, store: FHIRStore, mongo_client: MongoClient
    ):
        """create() correctly inserts a document in the database """
        with open("test/fixtures/patient-example.json") as f:
            patient = json.load(f)
            store.create(patient)

    def test_create_resource_with_extension(
        self, store: FHIRStore, mongo_client: MongoClient
    ):
        """create() correctly inserts a document in the database """
        with open("test/fixtures/patient-example-with-extensions.json") as f:
            patient = json.load(f)
            with raises(ValidationError):
                store.create(patient)
