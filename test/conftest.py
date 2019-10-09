import json
import pytest

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from fhirstore import FHIRStore

DB_NAME = "fhirstore_test"


@pytest.fixture(scope="session")
def store():
    client = MongoClient(serverSelectionTimeoutMS=5)
    try:
        client.server_info()
    except ServerSelectionTimeoutError as err:
        print("MongoClient could not reach server, is it running ?")
        raise

    fhirstore = FHIRStore(client, DB_NAME)
    fhirstore.reset()
    fhirstore.bootstrap(depth=4, resource="Patient")
    return fhirstore


@pytest.fixture(scope="session")
def mongo_client():
    return MongoClient()[DB_NAME]


@pytest.fixture(scope="function")
def test_patient(mongo_client):
    with open("test/fixtures/patient-example.json") as f:
        patient = json.load(f)
        yield patient

        # delete patient if and "_id" is present
        if patient.get("_id"):
            mongo_client["Patient"].delete_one({"_id": patient["_id"]})
