import unittest

from ExoplanetDataRepository import ExoplanetDataRepository


class TestExoplanetDataRepository(unittest.TestCase):
    def test_get_all_data_in_a_csv_file(self):
        api_stub_url = "http://192.168.1.10:3000/download"
        data_repo = ExoplanetDataRepository(api_stub_url)
        data = data_repo.get_all()
        print(data)
        print(len(data))
        self.assertNotEqual(data_repo, [])
        self.assertEqual(len(data), 3)
