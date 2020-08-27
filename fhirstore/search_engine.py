from typing import Optional

import elasticsearch

from fhirpath.connectors import create_connection
from fhirpath.engine.es import ElasticsearchEngine as BaseEngine
from fhirpath.engine import dialect_factory
from fhirpath.enums import FHIR_VERSION, EngineQueryType
from fhirpath_helpers.elasticsearch.mapping import generate_mappings


class ElasticSearchEngine(BaseEngine):
    def __init__(self, fhir_release, conn_factory, dialect_factory):
        super().__init__(fhir_release, conn_factory, dialect_factory)
        self.mappings = generate_mappings(FHIR_VERSION.R4.name)

    def calculate_field_index_name(self, resource_type):
        return resource_type

    def current_url(self):
        """
        complete url from current request
        return yarl.URL"""
        from yarl import URL

        return URL("https://dev.arkhn.com")

    def get_index_name(self):
        """ """
        return "fhirstore"

    def get_mapping(self, resource_type: str, index_config=None):
        """ """
        try:
            return {"properties": self.mappings[resource_type]}
        except KeyError as e:
            raise Exception(f"resource_type {e} does not exist in elasticsearch mappings")

    def create_es_index(self, resource=None):
        body = {
            "settings": {
                "index.mapping.total_fields.limit": 10000,
                "index.mapping.nested_fields.limit": 10000,
                "analysis": {
                    "normalizer": {"fhir_normalizer": {"filter": ["lowercase", "asciifolding"]}},
                    "analyzer": {
                        "path_analyzer": {"tokenizer": "path_tokenizer"},
                        "fhir_reference_analyzer": {"tokenizer": "fhir_reference_tokenizer"},
                    },
                    "tokenizer": {
                        "path_tokenizer": {"delimiter": "/", "type": "path_hierarchy"},
                        "fhir_reference_tokenizer": {
                            "type": "pattern",
                            "pattern": "(?:\w+\/)?(https?\:\/\/.*|\w+)",
                            "group": 1,
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
        # otherwise create it
        else:
            self.connection._conn.indices.create(self.get_index_name(), body=body)

        self.connection._conn.indices.refresh(index=self.get_index_name())


def create_search_engine(es_client: elasticsearch.Elasticsearch):
    es_node = es_client.transport.hosts[0]
    connection = create_connection(
        f"es://{es_node.get('http_auth', 'elastic')}@{es_node['host']}:{es_node['port']}/",
        "elasticsearch.Elasticsearch",
    )
    return ElasticSearchEngine(FHIR_VERSION.R4, lambda x: connection, dialect_factory)
