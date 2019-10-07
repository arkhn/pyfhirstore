import pytest
from pymongo import MongoClient
from fhirstore import FHIRStore

DB_NAME = "fhirstore_test"

@pytest.fixture
def fresh_store():
    client = MongoClient()
    fhirstore = FHIRStore(client, DB_NAME)
    return fhirstore

@pytest.fixture
def mongo_client():
    return MongoClient()

