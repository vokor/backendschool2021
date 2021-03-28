import os
import unittest
from unittest.mock import MagicMock

import mockupdb
from bson import json_util
from jsonschema import ValidationError
from pymongo import MongoClient

from index import make_app
from tests import test_utils


class OrdersPostTests(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.app, cls.db, cls.validator = test_utils.set_up_service()

    def test_successful_orders_post_should_return_list_ids(self):
        headers = [('Content-Type', 'application/json')]
        orders_data = test_utils.read_orders_data()

        http_response = self.app.post('/orders', data=json_util.dumps(orders_data), headers=headers)

        response_data = http_response.get_json()

        orders_list = []
        for order in orders_data['data']:
            orders_list.append({'id': order['order_id']})

        self.assertEqual(http_response.status_code, 201)
        self.assertEqual(orders_data["data"], self.db['orders'].find_one()["data"]) # TODO: check this
        self.assertEqual({'orders': orders_list}, response_data)

    def test_when_no_content_type_should_return_bad_request(self):
        http_response = self.app.post('/orders', data=json_util.dumps({'test': 1}))

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Content-Type must be application/json', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_database_error_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]
        orders_data = {'_id': '5a8f1e368f7936badfbb0cfa'}

        self.app.post('/orders', data=json_util.dumps(orders_data), headers=headers)
        http_response = self.app.post('/orders', data=json_util.dumps(orders_data), headers=headers)

        http_data = http_response.get_data(as_text=True)
        self.assertIn('Database error: ', http_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_incorrect_json_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]

        http_response = self.app.post('/orders', data='{', headers=headers)

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Error when parsing JSON: ', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_duplicate_id_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]
        orders_data = test_utils.read_orders_data()
        self.app.post('/orders', data=json_util.dumps(orders_data), headers=headers)
        http_response = self.app.post('/orders', data=json_util.dumps(orders_data), headers=headers)

        response_data = http_response.get_data(as_text=True)

        self.assertEqual(400, http_response.status_code)
        self.assertIn('Database error: ', response_data)

    def test_when_invalid_orders_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]
        mock_validation = MagicMock(side_effect=ValidationError('message'))
        with unittest.mock.patch.object(self.validator, 'validate_orders', mock_validation): # TODO change data
            req = {'data': [{'order_id': 1, 'order_type': 'bike', 'regions': [], 'working_hours': []},
                            {'order_id': 1, 'order_type': 'bike', 'regions': [], 'working_hours': []}]}

            http_response = self.app.post('/orders', data=json_util.dumps(req), headers=headers)

            response_data = http_response.get_json()  # TODO have problems

            self.assertEqual(1, response_data['validation_error']['orders'][0]['id'])
            self.assertEqual(2, response_data['validation_error']['orders'][1]['id'])
            self.assertEqual(400, http_response.status_code)


if __name__ == '__main__':
    unittest.main()
