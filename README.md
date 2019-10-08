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

# Insert resources
store.create({
    "resourceType": "Patient",
    "gender": "male",
})

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
