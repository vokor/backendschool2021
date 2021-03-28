import os
import unittest
from unittest.mock import MagicMock

import mockupdb
from bson import json_util
from iso8601 import iso8601
from jsonschema import ValidationError
from parameterized import parameterized
from pymongo import MongoClient

from index import make_app
from parser import parse_hours
from preparer import prepare_couriers, prepare_orders, prepare_order
from tests import test_utils

# TODO: add tests for Courier assign values, check order data (status...)
class CompletePostTests(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.app, cls.db, cls.validator = test_utils.set_up_service()

        couriers_data = test_utils.read_couriers_data()
        parse_hours(couriers_data, 'working_hours')
        data_to_insert = prepare_couriers(couriers_data)
        cls.db['couriers'].insert_many(data_to_insert)

        orders_data = test_utils.read_orders_data()
        parse_hours(orders_data, 'delivery_hours')
        data_to_insert = prepare_orders(orders_data)
        cls.db['orders'].insert_many(data_to_insert)

    def add_correct_order(self):
        assign_time = iso8601.parse_date('2021-01-10T09:33:01.42Z')
        order_data = prepare_order(order_id=33, weight=3, region=12,
                                   delivery_hours=["10:00-10:30"], status='in_progress',
                                   courier_id=1, assign_time=assign_time)
        self.db['orders'].insert_one(order_data)

    def test_successful_orders_complete_post_should_return_order_id(self):
        headers = [('Content-Type', 'application/json')]
        self.add_correct_order()
        complete_data = {'courier_id': 1, 'order_id': 33, 'complete_time': '2021-01-10T10:20:01.42Z'}
        http_response = self.app.post('/orders/complete', data=json_util.dumps(complete_data), headers=headers)
        response_data = http_response.get_json()
        self.assertEqual(201, http_response.status_code)
        self.assertEqual({'order_id': 33}, response_data)

    @parameterized.expand([
        ({'courier_id': 50, 'order_id': 33, 'complete_time': '2021-01-10T10:20:01.42Z'}, 'Courier'),
        ({'courier_id': 2, 'order_id': 50, 'complete_time': '2021-01-10T10:33:01.42Z'}, 'Order'),
    ])
    def test_should_return_bad_request_when_order_or_courier_id_incorrect(self, complete_data: dict, object_name: str):
        headers = [('Content-Type', 'application/json')]
        self.add_correct_order()
        http_response = self.app.post('/orders/complete', data=json_util.dumps(complete_data), headers=headers)
        http_data = http_response.get_data(as_text=True)
        self.assertEqual(400, http_response.status_code)
        self.assertIn(f'{object_name} with specified id not found', http_data)

    def test_should_return_bad_request_when_order_assigned_on_another_courier(self):
        headers = [('Content-Type', 'application/json')]
        self.add_correct_order()
        complete_data = {'courier_id': 2, 'order_id': 33, 'complete_time': '2021-01-10T10:20:01.42Z'}
        http_response = self.app.post('/orders/complete', data=json_util.dumps(complete_data), headers=headers)
        http_data = http_response.get_data(as_text=True)
        self.assertEqual(400, http_response.status_code)
        self.assertIn('Order with specified id not found', http_data)


if __name__ == '__main__':
    unittest.main()
