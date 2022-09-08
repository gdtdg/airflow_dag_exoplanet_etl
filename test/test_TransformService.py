import json
import unittest

from TransformService import TransformService
from ExoplanetDataRepository import ExoplanetDataRepository
from InMemoryRepository import InMemoryRepository


class TestTransformService(unittest.TestCase):
    def test_no_data_no_transform(self):
        # Given
        in_repo = InMemoryRepository([])
        out_repo = InMemoryRepository([])

        # When
        service = TransformService(in_repo, out_repo)
        service.process()

        # Then
        out_data = out_repo.get_all()
        self.assertEqual(out_data, [])

    def test_one_data_is_transformed_correctly(self):
        # Given
        with open("resources/in", 'r') as file:
            in_data = json.loads(file.read())
        in_repo = InMemoryRepository([in_data])
        out_repo = InMemoryRepository([])

        # When
        service = TransformService(in_repo, out_repo)
        service.process()

        # Then
        out_data = out_repo.get_all()[0]
        self.assertNotEqual(out_data, in_data)

        self.assertEqual(out_data.get('pl_pubdate'), "2013-08")
        self.assertEqual(out_data.get('disc_pubdate'), "2013-08-01")
        self.assertEqual(out_data.get("releasedate"), "2014-05-14")
        self.assertEqual(out_data.get("rowupdate"), "2014-05-14")
        self.assertIn("rowid", out_data)

    def test_get_data_from_api_is_transformed(self):
        exoplanet_data_repo = ExoplanetDataRepository("http://192.168.1.10:3000/download")
        out_repo = InMemoryRepository([])
        service = TransformService(exoplanet_data_repo, out_repo)
        service.process()
        out_data = out_repo.get_all()[0]
        self.assertIn("rowid", out_data)
        self.assertEqual(out_data.get('pl_pubdate'), "2008-01")
        self.assertEqual(out_data.get('disc_pubdate'), "2008-01")
        self.assertEqual(out_data.get("releasedate"), "2014-05-14")
        self.assertEqual(out_data.get("rowupdate"), "2014-05-14")