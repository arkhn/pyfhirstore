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
        # return f"fhirstore.{resource_type.lower()}"
        return "fhirstore"

    def get_mapping(self, resource_type: str, index_config=None):
        """ """
        try:
            return {"properties": self.mappings[resource_type]}
        except KeyError as e:
            raise Exception(f"resource_type {e} does not exist in elasticsearch mappings")

    def _execute(self, query, unrestricted, query_type):
        """ """
        # for now we support single from resource
        query_copy = query.clone()
        resource_type = query.get_from()[0][1].resource_type
        field_index_name = self.calculate_field_index_name(resource_type)

        if unrestricted is False:
            self.build_security_query(query_copy)

        params = {
            "query": query_copy,
            "root_replacer": field_index_name,
            "mapping": self.get_mapping(resource_type),
        }

        compiled = self.dialect.compile(**params)
        if query_type == EngineQueryType.DML:
            raw_result = self.connection.fetch(self.get_index_name(), compiled)
        elif query_type == EngineQueryType.COUNT:
            raw_result = self.connection.count(self.get_index_name(), compiled)
        else:
            raise NotImplementedError

        return raw_result, field_index_name, compiled

    def create_es_index(self):
        body = {
            "settings": {
                "index.mapping.total_fields.limit": 10000,
                "index.mapping.nested_fields.limit": 10000,
                "analysis": {
                    "analyzer": {
                        "path_analyzer": {"tokenizer": "path_tokenizer"},
                        "fhir_reference_analyzer": {"tokenizer": "fhir_reference_tokenizer"},
                    },
                    "char_filter": {},
                    "filter": {},
                    "tokenizer": {
                        "path_tokenizer": {"delimiter": "/", "type": "path_hierarchy"},
                        "fhir_reference_tokenizer": {"type": "pattern", "pattern": "/"},
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

        for resource_type in self.mappings.keys():
            body["mappings"]["properties"][resource_type] = self.get_mapping(resource_type)

        self.connection._conn.indices.create(self.get_index_name(), body=body)
        self.connection._conn.indices.refresh(index=self.get_index_name())


def create_search_engine(es_client: elasticsearch.Elasticsearch):
    es_node = es_client.transport.hosts[0]
    connection = create_connection(
        f"es://{es_node.get('http_auth', 'elastic')}@{es_node['host']}:{es_node['port']}/",
        "elasticsearch.Elasticsearch",
    )
    return ElasticSearchEngine(FHIR_VERSION.R4, lambda x: connection, dialect_factory)
