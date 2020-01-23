import json
import pytest
import os

from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from fhirstore import FHIRStore

DB_NAME = "fhirstore_test"
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
CLIENT_PASSWORD = os.getenv("CLIENT_PASSWORD")
ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = os.getenv("ES_PORT", "9200")


@pytest.fixture(scope="session")
def store():
    client = MongoClient(username=MONGO_USERNAME, password=CLIENT_PASSWORD)
    try:
        client.server_info()
    except ServerSelectionTimeoutError as err:
        print("MongoClient could not reach server, is it running ?")
        raise
    client_es = Elasticsearch(
        [DB_HOST], http_auth=("elastic", CLIENT_PASSWORD), scheme="http", port=ES_PORT
    )

    fhirstore = FHIRStore(client, client_es, DB_NAME)
    fhirstore.reset()
    fhirstore.bootstrap(depth=4, resource="Patient")
    return fhirstore


@pytest.fixture(scope="session")
def mongo_client():
    return MongoClient(username=MONGO_USERNAME, password=CLIENT_PASSWORD)[DB_NAME]


@pytest.fixture(scope="function")
def test_patient(mongo_client):
    with open("test/fixtures/patient-example.json") as f:
        patient = json.load(f)
        yield patient

        if patient.get("_id"):
            mongo_client["Patient"].delete_one({"_id": patient["_id"]})
