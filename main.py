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
    store.bootstrap(depth=3)
    end = timer()
    print(end - start, "seconds")

    # creating document
    print("Inserting documents...")
    json_folder_path = os.path.join(os.getcwd(), "test/fixtures")
    json_files = [x for x in os.listdir(
        json_folder_path) if x.endswith(".json")]
    for json_file in json_files:
        json_file_path = os.path.join(json_folder_path, json_file)
        with open(json_file_path, "r") as f:
            data = json.load(f)
            start = timer()
            print("Creating", json_file_path, "...")
            try:
                store.create(data)
            except Exception as e:
                print(e)
            end = timer()
            print(end - start, "seconds")

    client.close()
