from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from Repository import Repository


class ExoplanetElasticSearchRepository(Repository):
    def __init__(self, es_index, es_connection, es_mapping):
        self.es_index = es_index
        es_url = es_connection.host + str(es_connection.port)
        es_auth = (es_connection.login, es_connection.password)
        es = Elasticsearch([es_url], basic_auth=es_auth, verify_certs=False)
        self.es = es
        self.es_mapping = es_mapping

    def delete_es_index(self):
        self.es.options(ignore_status=[400, 404]).indices.delete(index=self.es_index)

    def create_index_with_mapping(self):
        self.es.indices.create(
            index=self.es_index,
            body=self.es_mapping
        )

    def add_all(self, items: list):
        actions = []
        for j in range(0, len(items)):
            action = {
                "_index": self.es_index,
                "_id": j,
                "_source": items[j]
            }
            actions.append(action)
        return bulk(self.es, actions)

    def get_all(self) -> list:
        request_body = {"query": {"match_all": {}}}
        ret = self.es.search(index=self.es_index, body=request_body)
        return ret
