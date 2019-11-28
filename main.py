import os
import time
import json
from timeit import default_timer as timer
from pymongo import MongoClient
from elasticsearch import Elasticsearch

from fhirstore import FHIRStore

if __name__ == '__main__':
    client = MongoClient(username="arkhn", password="SuperSecurePassword2019")

    client_es = Elasticsearch(
            ['http://localhost:9200'],
            http_auth =("elastic", "SuperSecurePassword2019")
    )
    # uncomment the next 2 following line if you wish to activate
    # the replication mode of mongo (required by monstache)

    # res = client.admin.command("replSetInitiate", None)
    # time.sleep(1)

    store = FHIRStore(client, client_es, "fhirstore")

    # uncomment the following line if mongo already has initialised
    # collections and you don't want to bootstrap them all
    # store.resume()

    # reset collections (comment if you used `store.resume()`)
    print("Dropping collections...")
    start = timer()
    store.reset()
    end = timer()
    print(end - start, "seconds")

    # create collections (comment if you used `store.resume()`)
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
    total = 0
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
            total += (end-start)
    print(f"Inserted {len(json_files)} documents in {total} seconds")
    
    # srch = store.search(resource='patient',params="male") 
    
    # print(srch)

    client.close()
