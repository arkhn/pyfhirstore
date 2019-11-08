# fhirstore
Python library to manipulate fhir resources leveraging mongoDB as storage layer.

## Installation

```bash
pip install fhirstore
```

## Usage

```python
from pymongo import MongoClient
from fhirstore import FHIRStore

client = MongoClient()
store = FHIRStore(client, "<my_database>")

# Dropping collections
store.reset()

# Parse json schema and create collections
store.bootstrap(depth=5)
# OR
# Get existing collections from the database
store.resume(depth=5)

# Create resources
store.create({
    "resourceType": "Patient",
    "id": "pat1",
    "gender": "male"
})

# Read resources
patient = store.read("Patient", "pat1")

# Update resources
updated_patient = store.update("Patient", "pat1", {"gender": "other"})

# Delete resources
deleted_patient_id = store.delete("Patient", "pat1")
```

## Bootstrap the database

1. Start the database
2. Drop and re-create all collections based on the provided schema

```bash
docker-compose up -d
python main.py
```

## Development setup

1. Create a virtual environment and enter it
2. Install python dependncies
   
```bash
virtualenv . 
. ./bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Test
Test can be run using :
```bash
python -m pytest
```
Make sure dev dependencies are installed.

## Benchmark
A benchmark based on example FHIR resources publicly available at can be run using :
```bash
python benchmark/benchmark.py
```
Note that you will need to have a MongoDB up in order for the benchmark to run. You can use the docker-compose file of this repository by running `docker-compose up` before launching the benchmark.

On a machine with 16GB RAM and an i7 (2.5GHz) processor, the results of the benchamrk were:
```
--- WRITES ---
insertions per second (on average): 267.17
average: 3.74 milliseconds
median: 1.66 milliseconds
min: 1.07 milliseconds
max: 724.65 milliseconds
spread: 0.00028004697751347234

--- READS ---
reads per second (on average): 378.93
average: 2.63 milliseconds
median: 1.50 milliseconds
min: 0.88 milliseconds
max: 481.18 milliseconds
spread: 0.0002154728657872756
```

## Publish

First, you need to have `twine` installedd
```
pip install --user --upgrade twine
```

Make sure you have bumped the version number in `setup.py`, then run the following:
```
python setup.py sdist bdist_wheel
python -m twine upload dist/*
```
