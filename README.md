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
