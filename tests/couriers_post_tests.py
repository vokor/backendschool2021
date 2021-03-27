import os
import unittest
from unittest.mock import MagicMock

import mockupdb
from bson import json_util
from jsonschema import ValidationError
from pymongo import MongoClient

from data_validator import DataValidator
from index import make_app


class CouriersPostTests(unittest.TestCase):
    @staticmethod
    def create_mock_validator() -> DataValidator:
        validator = DataValidator()
        validator.validate_couriers = MagicMock()
        return validator

    @classmethod
    def setUpClass(cls):
        cls.server = mockupdb.MockupDB(auto_ismaster=True)
        cls.server.run()
        cls.db = MongoClient(cls.server.uri)['db']
        cls.validator = cls.create_mock_validator()
        cls.app = make_app(cls.db, cls.validator).test_client()

    @staticmethod
    def read_couriers_data():
        document_id = '5a8f1e368f7936badfbb0cfa'
        with open(os.path.join(os.path.dirname(__file__), 'couriers.json')) as f:
            couriers_data = json_util.loads(f.read())
        couriers_data['_id'] = document_id
        return couriers_data

    def test_successful_couriers_post_should_return_list_ids(self):
        headers = [('Content-Type', 'application/json')]
        couriers_data = self.read_couriers_data()
        document_id = str(couriers_data['_id'])

        future = mockupdb.go(self.app.post, '/couriers', data=json_util.dumps(couriers_data), headers=headers)
        if self.server.got(mockupdb.OpMsg({'count': 'couriers'}, namespace='db')):
            self.server.ok(n=0)
        if self.server.got(mockupdb.OpMsg({'insert': 'couriers', 'documents': [couriers_data]}, namespace='db')):
            self.server.ok(cursor={'inserted_id': document_id})

        http_response = future()

        response_data = http_response.get_json()

        couriers_list = []
        for courier in couriers_data['data']:
            couriers_list.append({'id': courier['courier_id']})

        self.assertEqual({'couriers': couriers_list}, response_data)
        self.assertEqual(http_response.status_code, 201)

    def test_when_no_content_type_should_return_bad_request(self):
        http_response = self.app.post('/couriers', data=json_util.dumps({'test': 1}))

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Content-Type must be application/json', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_database_error_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]
        couriers_data = self.read_couriers_data()

        future = mockupdb.go(self.app.post, '/couriers', data=json_util.dumps(couriers_data), headers=headers)
        if self.server.got(mockupdb.OpMsg({'count': 'couriers'}, namespace='db')):
            self.server.ok(n=0)
        if self.server.got(mockupdb.OpMsg({'insert': 'couriers', 'documents': [couriers_data]}, namespace='db')):
            self.server.command_err(11000, 'message')

        http_response = future()
        http_data = http_response.get_data(as_text=True)
        self.assertIn('Database error: ', http_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_incorrect_json_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]

        http_response = self.app.post('/couriers', data='{', headers=headers)

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Error when parsing JSON: ', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_invalid_couriers_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]
        mock_validation = MagicMock(side_effect=ValidationError('message'))
        with unittest.mock.patch.object(self.validator, 'validate_couriers', mock_validation):
            req = {'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [], 'working_hours': []},
                            {'courier_id': 1, 'courier_type': 'bike', 'regions': [], 'working_hours': []}]}

            http_response = self.app.post('/couriers', data=json_util.dumps(req), headers=headers)

            response_data = http_response.get_json() #TODO have problems

            self.assertEqual(1, response_data['validation_error']['couriers'][0]['id'])
            self.assertEqual(2, response_data['validation_error']['couriers'][1]['id'])
            self.assertEqual(400, http_response.status_code)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
