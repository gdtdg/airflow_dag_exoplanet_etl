import json
import unittest
from pprint import pprint

from ExoplanetElasticSearchRepository import ExoplanetElasticSearchRepository


class EsConnection:
    def __init__(self, host, port, login, password):
        self.host = host
        self.port = port
        self.login = login
        self.password = password


class TestExoplanetElasticsearchRepository(unittest.TestCase):
    def test_one_elasticsearch_index_is_get(self):
        # Given
        es_connection = EsConnection("https://192.168.1.10:", "9200", "elastic", "5jzRQJp7NelEvwectkHm")
        es = ExoplanetElasticSearchRepository("stub_index_for_exoplanet_dag", es_connection)
        self.assertEqual(es_connection.port, "9200")
        # When
        list_test = es.get_all()
        pprint(list_test)
        # Then
        self.assertNotEqual(list_test, [])

    def test_add_all_data_in_elasticsearch(self):
        # Given
        es_connection = EsConnection("https://192.168.1.10:", "9200", "elastic", "5jzRQJp7NelEvwectkHm")
        es = ExoplanetElasticSearchRepository("stub_index_for_exoplanet_dag", es_connection)
        with open("resources/data_to_add_for_elasticsearch.json", 'r') as file:
            data_to_add = json.loads(file.read())
        # WHen
        success_count, errors = es.add_all(data_to_add)
        print("add all data: ", success_count, errors)
        # Then
        self.assertEqual(success_count, len(data_to_add))
