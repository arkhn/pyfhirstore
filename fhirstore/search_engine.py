from glob import glob
import os
import json
import logging
from yarl import URL

from elasticsearch.exceptions import NotFoundError as ESNotFoundError

from fhirstore.errors import SearchEngineError

from fhirpath.connectors.factory.es import ElasticsearchConnection
from fhirpath.engine.es import ElasticsearchEngine as BaseEngine
from fhirpath.engine import dialect_factory, EngineResultRow

FHIR_API_URL = os.getenv("FHIR_API_URL", "https://arkhn.com")
ELASTIC_MAPPING_FILES_DIR = os.getenv("ELASTIC_MAPPING_FILES_DIR")


class ElasticSearchEngine(BaseEngine):

    es_reference_analyzer = "fhir_reference_analyzer"
    es_token_normalizer = "fhir_normalizer"

    def __init__(self, fhir_release, es_client, es_index):
        super().__init__(
            fhir_release, lambda x: ElasticsearchConnection(es_client), dialect_factory
        )
        self.es_index = es_index
        self.mappings = {}

        # load the ES mappings from static files
        if ELASTIC_MAPPING_FILES_DIR:
            logging.info(f"Loading ES index from {ELASTIC_MAPPING_FILES_DIR}...")
            try:
                mapping_files = glob(f"{ELASTIC_MAPPING_FILES_DIR}/**.json")
                for filename in mapping_files:
                    with open(filename, "r") as f:
                        try:
                            es_mapping = json.load(f)
                            resource_type = es_mapping.get("resourceType")
                            self.mappings[resource_type] = es_mapping.get("mapping")
                        except Exception as err:
                            raise SearchEngineError(f"{filename} is not a valid JSON file: {err}")

            except FileNotFoundError as e:
                raise SearchEngineError(f"Could not find ES mapping file: {e}")
            except IsADirectoryError:
                raise SearchEngineError(f"ELASTIC_MAPPING_FILES_DIR should contain only JSON files")

        # load the ES mappings dynamically
        else:
            logging.info(
                "ELASTIC_MAPPING_FILES_DIR was not found in environment, "
                "generating mappings dynamically"
            )
            self.mappings = self.generate_mappings(
                self.es_reference_analyzer, self.es_token_normalizer
            )

    def calculate_field_index_name(self, resource_type):
        return resource_type

    def current_url(self):
        """
        complete url from current request
        return yarl.URL"""
        return URL(FHIR_API_URL)

    def get_index_name(self):
        """ """
        return self.es_index

    def get_mapping(self, resource_type: str, index_config=None):
        """ """
        try:
            return self.mappings[resource_type]
        except KeyError as e:
            raise Exception(f"resource_type {e} does not exist in elasticsearch mappings")

    def reset(self):
        try:
            self.connection._conn.indices.delete(self.get_index_name())
        except ESNotFoundError:
            logging.warning(f"index {self.get_index_name()} does not exist, skipping...")

    def create_es_index(self, resource=None):
        body = {
            "settings": {
                "index.mapping.total_fields.limit": 100000,
                "index.mapping.nested_fields.limit": 10000,
                "analysis": {
                    "normalizer": {
                        self.es_token_normalizer: {"filter": ["lowercase", "asciifolding"]}
                    },
                    "analyzer": {
                        self.es_reference_analyzer: {
                            "tokenizer": "keyword",
                            "filter": ["fhir_reference_filter"],
                        },
                    },
                    "filter": {
                        "fhir_reference_filter": {
                            "type": "pattern_capture",
                            "preserve_original": True,
                            "patterns": [r"(?:\w+\/)?(https?\:\/\/.*|[a-zA-Z0-9_-]+)"],
                        },
                    },
                },
            },
            "mappings": {
                "dynamic": False,
                "properties": {
                    "elastic_index": {"index": True, "store": True, "type": "keyword"},
                    "id": {"index": True, "store": True, "type": "keyword"},
                },
            },
        }
        if resource and resource not in self.mappings.keys():
            raise Exception(f"cannot index resource {resource}, elasticsearch mapping not found")

        init_indices = [resource] if resource else self.mappings.keys()
        for resource_type in init_indices:
            body["mappings"]["properties"][resource_type] = self.get_mapping(resource_type)

        # if the index already exist, update it
        if self.connection._conn.indices.exists(self.get_index_name()):
            self.connection._conn.indices.put_mapping(
                body=body["mappings"], index=self.get_index_name()
            )
            self.connection._conn.indices.refresh(index=self.get_index_name())

        # otherwise create it
        else:
            self.connection._conn.indices.create(self.get_index_name(), body=body)
            self.connection._conn.indices.refresh(
                index=self.get_index_name(), ignore_unavailable=True
            )

    def extract_hits(self, source_filters, hits, container, doc_type="_doc"):
        """ """
        for res in hits:
            if res["_type"] != doc_type:
                continue
            row = EngineResultRow()

            # the res["_source"] object contains the resource data indexed by resource type.
            # eg: {"Patient": {patient_data...}}
            # this object should always have a single key:value pair since the term queries
            # performed by ES are always scoped by resource_type.
            # In short, row is an array with a single item.
            for field_index_name, resource_data in res["_source"].items():
                row.append({**resource_data, "resourceType": field_index_name})

            container.add(row)
