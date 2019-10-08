import os
import json
from timeit import default_timer as timer
from pymongo import MongoClient

from fhirstore import FHIRStore

if __name__ == '__main__':
    client = MongoClient()
    store = FHIRStore(client, "fhirstore")

    # reset collections
    print("Dropping collections...")
    start = timer()
    store.reset()
    end = timer()
    print(end - start, "seconds")

    # create collections
    print("Creating collections...")
    start = timer()
    store.bootstrap(depth=5)
    end = timer()
    print(end - start, "seconds")

    # creating document
    print("Inserting document...")
    start = timer()
    store.create({
        "resourceType": "Patient",
        "gender": "male",
    })
    end = timer()
    print(end - start, "seconds")

    client.close()
