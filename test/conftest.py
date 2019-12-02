import json
import pytest

# import os

from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from fhirstore import FHIRStore

DB_NAME = "fhirstore_test"
# MONGO_USERNAME = os.getenv("MONGO_USERNAME")
# CLIENT_PASSWORD = os.getenv("CLIENT_PASSWORD")


@pytest.fixture(scope="session")
def store():
    client = MongoClient(username="arkhn", password="SuperSecurePassword2019")
    # serverSelectionTimeoutMS=5)
    try:
        client.server_info()
    except ServerSelectionTimeoutError as err:
        print("MongoClient could not reach server, is it running ?")
        raise
    client_es = Elasticsearch(
        ["http://localhost:9200"], http_auth=("elastic", "SuperSecurePassword2019")
    )

    fhirstore = FHIRStore(client, client_es, DB_NAME)
    fhirstore.reset()
    fhirstore.bootstrap(depth=4, resource="Patient")
    return fhirstore


@pytest.fixture(scope="session")
def mongo_client():
    return MongoClient(username="arkhn", password="SuperSecurePassword2019")[DB_NAME]
    # serverSelectionTimeoutMS=5)[DB_NAME]


# @pytest.fixture(scope="session")
# def es_client():
#     return Elasticsearch()[DB_NAME]


@pytest.fixture(scope="function")
def test_patient(mongo_client):
    with open("test/fixtures/patient-example.json") as f:
        patient = json.load(f)
        yield patient

        # delete patient if and "_id" is present
        if patient.get("_id"):
            mongo_client["Patient"].delete_one({"_id": patient["_id"]})
