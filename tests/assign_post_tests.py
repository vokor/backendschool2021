import logging
import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock

import mockupdb
from bson import json_util
from iso8601 import iso8601
from jsonschema import ValidationError
from parameterized import parameterized
from pymongo import MongoClient

from index import make_app
from parser import parse_hours
from preparer import prepare_orders, prepare_couriers
from tests import test_utils


class AssignPostTests(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.app, cls.db, cls.validator = test_utils.set_up_service()
        logging.disable(logging.CRITICAL)

        orders_data = test_utils.read_data('orders.json')
        parse_hours(orders_data, 'delivery_hours')
        data_to_insert = prepare_orders(orders_data)
        cls.db['orders'].insert_many(data_to_insert)

    def add_courier(self, courier):
        parse_hours(courier, 'working_hours')  # FIXME: remove it!!!
        data_to_insert = prepare_couriers(courier)
        self.db['couriers'].insert_many(data_to_insert)

    @parameterized.expand([
        ({'data': [{'courier_id': 4, 'courier_type': 'foot', 'regions': [5, 22, 12], 'working_hours': ['10:00-11:00']}]}, [1, 3]),
        ({'data': [{'courier_id': 4, 'courier_type': 'foot', 'regions': [5, 22, 12], 'working_hours': ['20:00-21:00']}]}, [3])
    ])
    def test_successful_assign_post_should_return_correct_response(self, courier: dict, order_ids: str):
        headers = [('Content-Type', 'application/json')]
        self.add_courier(courier)
        assign_data = {'courier_id': 4}
        http_response = self.app.post('/orders/assign', data=json_util.dumps(assign_data), headers=headers)
        response_data = http_response.get_json()
        expected_result = []
        for order_id in order_ids:
            expected_result.append({'id': order_id})
        self.assertEqual(201, http_response.status_code)
        self.assertEqual(expected_result, response_data['orders'])

    def test_assign_post_should_return_nothing_when_empty_list(self):
        headers = [('Content-Type', 'application/json')]
        courier = {'data': [{'courier_id': 4, 'courier_type': 'foot', 'regions': [5, 22, 12], 'working_hours': ['00:00-02:00']}]}
        self.add_courier(courier)
        assign_data = {'courier_id': 4}
        http_response = self.app.post('/orders/assign', data=json_util.dumps(assign_data), headers=headers)
        response_data = http_response.get_json()
        self.assertEqual(201, http_response.status_code)
        self.assertEqual([], response_data['orders'])
        self.assertFalse('assign_time' in response_data)

    def test_assign_post_should_not_return_completed_orders(self):
        headers = [('Content-Type', 'application/json')]
        courier = {'data': [{'courier_id': 4, 'courier_type': 'foot', 'regions': [5, 22, 12], 'working_hours': ['10:00-11:00']}]}
        self.add_courier(courier)
        assign_data = {'courier_id': 4}
        self.app.post('/orders/assign', data=json_util.dumps(assign_data), headers=headers)
        order = self.db['orders'].find_one(filter={'_id': 1})
        update_data = {
            '$set': {
                'complete_time': datetime.now(tz=None),
                'status': 'completed'
            }
        }
        self.db['orders'].update_one(filter={'_id': 1}, update=update_data)
        http_response = self.app.post('/orders/assign', data=json_util.dumps(assign_data), headers=headers)
        response_data = http_response.get_json()
        self.assertEqual(201, http_response.status_code)
        self.assertEqual([{'id': 3}], response_data['orders'])
        self.assertEqual(order['assign_time'], response_data['assign_time'])

    def test_assign_should_be_idempotency(self):
        headers = [('Content-Type', 'application/json')]
        courier = {'data': [{'courier_id': 4, 'courier_type': 'foot', 'regions': [5, 22, 12], 'working_hours': ['10:00-11:00']}]}
        self.add_courier(courier)
        assign_data = {'courier_id': 4}
        self.app.post('/orders/assign', data=json_util.dumps(assign_data), headers=headers)
        http_response = self.app.post('/orders/assign', data=json_util.dumps(assign_data), headers=headers)
        response_data = http_response.get_json()
        self.assertEqual(201, http_response.status_code)
        self.assertEqual([{'id': 1}, {'id': 3}], response_data['orders'])


if __name__ == '__main__':
    unittest.main()
