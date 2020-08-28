import json
import pytest
import os

from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from fhirstore import FHIRStore

DB_NAME = "fhirstore_test"
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
ES_PASSWORD = os.getenv("ES_PASSWORD")
ES_URL = os.getenv("ES_URL")


@pytest.fixture(scope="session")
def store():
    client = MongoClient(username=MONGO_USERNAME, password=MONGO_PASSWORD)
    try:
        client.server_info()
    except ServerSelectionTimeoutError:
        print("MongoClient could not reach server, is it running ?")
        raise
    client_es = Elasticsearch([ES_URL])

    fhirstore = FHIRStore(client, client_es, DB_NAME)
    fhirstore.reset()
    fhirstore.bootstrap(resource="Appointment")
    fhirstore.bootstrap(resource="CodeSystem")
    fhirstore.bootstrap(resource="Location")
    fhirstore.bootstrap(resource="MedicationRequest")
    fhirstore.bootstrap(resource="MolecularSequence")
    fhirstore.bootstrap(resource="Observation")
    fhirstore.bootstrap(resource="Patient")
    fhirstore.bootstrap(resource="Practitioner")

    return fhirstore


@pytest.fixture(scope="session")
def mongo_client():
    return MongoClient(username=MONGO_USERNAME, password=MONGO_PASSWORD)[DB_NAME]


@pytest.fixture(scope="session")
def es_client():
    return Elasticsearch([ES_URL])


@pytest.fixture(scope="function")
def test_patient(mongo_client):
    with open("test/fixtures/patient-example.json") as f:
        patient = json.load(f)
        yield patient

        if patient.get("_id"):
            mongo_client["Patient"].delete_one({"_id": patient["_id"]})


@pytest.fixture(scope="function")
def test_bundle(mongo_client):
    with open("test/fixtures/bundle-example.json") as f:
        bundle = json.load(f)
        yield bundle
