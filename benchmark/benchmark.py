import requests
import io
import zipfile
import json
from os import path
from timeit import default_timer as timer
import statistics
from uuid import uuid4

from tqdm import tqdm
from pymongo import MongoClient
from fhirstore import FHIRStore

example_blacklist = [
    "package-min-ver.json",
    "profiles-resources.json",
    "questionnaireresponse-extensions-QuestionnaireResponse-item-subject.json"]


def count_examples():
    with zipfile.ZipFile('benchmark/examples.zip') as archive:
        return len([
            f for f in archive.infolist()
            if f.filename not in example_blacklist
        ])


def iter_examples():
    with zipfile.ZipFile('benchmark/examples.zip') as archive:
        for zipinfo in archive.infolist():
            with archive.open(zipinfo) as thefile:
                if zipinfo.filename not in example_blacklist:
                    yield zipinfo.filename, json.load(thefile)


def download_resources():
    """
    Downloads examples from HL7 website.
    """
    if not path.exists('benchmark/examples.zip'):
        url = "http://www.hl7.org/fhir/examples-json.zip"
        r = requests.get(url, stream=True)
        total_size = int(r.headers.get('content-length', 0))
        block_size = 1024
        t = tqdm(total=total_size, unit='B', unit_scale=True,
                 desc="Downloading example resources")
        with open('benchmark/examples.zip', 'wb') as f:
            for data in r.iter_content(block_size):
                t.update(len(data))
                f.write(data)
        t.close()
    else:
        print("Using cached resources")


download_resources()
client = MongoClient()
store = FHIRStore(client, "benchmark")
store.resume()
# store.reset()
# store.bootstrap(depth=3)

examples = tqdm(iter_examples(),
                total=count_examples(),
                desc="Running write benchmark")
stats = {}
inserted = []
for example, data in examples:
    if not data.get('id'):
        data["id"] = str(uuid4())
    start = timer()
    res = store.create(data)
    end = timer()
    stats[example] = end - start
    inserted.append(res)

values = stats.values()
print(f"insertions per second (on average): {1/statistics.mean(values):.2f}")
print(f"average: {statistics.mean(values)*1000:.2f} milliseconds")
print(f"median: {statistics.median(values)*1000:.2f} milliseconds")
print(f"min: {min(values)*1000:.2f} milliseconds")
print(f"max: {max(values)*1000:.2f} milliseconds")
print(f"spread: {statistics.variance(values)}")

examples = tqdm(inserted,
                desc="Running read benchmark")
stats = {}
for doc in examples:
    start = timer()
    store.read(doc["resourceType"], doc["id"])
    end = timer()
    stats[doc["id"]] = end - start

values = stats.values()
print(f"reads per second (on average): {1/statistics.mean(values):.2f}")
print(f"average: {statistics.mean(values)*1000:.2f} milliseconds")
print(f"median: {statistics.median(values)*1000:.2f} milliseconds")
print(f"min: {min(values)*1000:.2f} milliseconds")
print(f"max: {max(values)*1000:.2f} milliseconds")
print(f"spread: {statistics.variance(values)}")
