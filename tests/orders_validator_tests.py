import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock

from bson import json_util
from jsonschema import ValidationError
from parameterized import parameterized

from data_validator import DataValidator


class OrdersValidatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_validator = DataValidator()

    def test_correct_orders_should_be_valid(self):
        with open(os.path.join(os.path.dirname(__file__), 'orders.json')) as f:
            orders_data = json_util.loads(f.read())
        self.data_validator.validate_orders(orders_data)

    def assert_exception(self, orders_data: dict, expected_exception_message: str):
        with self.assertRaises(ValidationError) as context:
            self.data_validator.validate_orders(orders_data)
        self.assertIn(expected_exception_message, str(context.exception.message))

    @parameterized.expand([({}, 'data')])
    def test_orders_should_be_incorrect_when_missing_data_field(self, orders_data: dict, field_name: str):
        self.assert_exception(orders_data, f'\'{field_name}\' is a required property')

    @parameterized.expand([
        [{'data': [{'order_id': 1, 'region': 4, 'delivery_hours': []}]}],
        [{'data': [{'order_id': 1, 'weight': 3, 'delivery_hours': []}]}],
        [{'data': [{'order_id': 1, 'weight': 3, 'region': 4}]}]
    ])
    def test_orders_should_be_incorrect_when_missing_field(self, orders_data: dict):
        self.assert_exception(orders_data, "{'orders': [{'id': 1}]}")

    @parameterized.expand([
        ({'data': None}, 'array'),
        ({'data': ['']}, 'object'),
    ])
    def test_orders_should_be_incorrect_when_wrong_type_of_field(self, orders_data: dict, data_type: str):
        self.assert_exception(orders_data, f'is not of type \'{data_type}\'')

    @parameterized.expand([
        [{'data': [{'order_id': 1, 'weight': None, 'region': 4, 'delivery_hours': []}]}],
        [{'data': [{'order_id': 1, 'weight': 3, 'region': None, 'delivery_hours': []}]}],
        [{'data': [{'order_id': 1, 'weight': 3, 'region': 4, 'delivery_hours': None}]}],
        [{'data': [{'order_id': 1, 'weight': 3, 'region': [''], 'delivery_hours': []}]}]
    ])
    def test_orders_should_be_incorrect_when_wrong_type_of_field(self, orders_data: dict):
        self.assert_exception(orders_data, "{'orders': [{'id': 1}]}")

    def test_orders_data_should_be_correct_with_different_field_order(self):
        orders_data = {'data': [{'delivery_hours': [], 'weight': 3, 'region': 4, 'order_id': 1}]}
        self.data_validator.validate_orders(orders_data)

    @parameterized.expand([
        ({'EXTRA': 0, 'data': [{'order_id': 1, 'weight': 3, 'region': 4, 'delivery_hours': ["00:59-23:59"]}]}, ''),
        ({'data': [{'EXTRA': 0, 'order_id': 1, 'weight': 3, 'region': 4, 'delivery_hours': ["00:59-23:59"]}]},
         "{'orders': [{'id': 1}]}"),
    ])
    def test_orders_should_be_incorrect_when_containing_extra_fields(self, orders_data: dict, field_name: str):
        self.assert_exception(orders_data, field_name)

    @unittest.mock.patch('jsonschema.validate')
    def test_orders_should_be_incorrect_when_order_ids_not_unique(self, _):
        orders_data = {'data': [{'order_id': 1}, {'order_id': 1}]}
        self.assert_exception(orders_data, 'Orders ids are not unique')

    @unittest.mock.patch('jsonschema.validate')
    def test_correct_delivery_hours_should_be_parsed(self, _):
        orders_data = {
            'data': [{'order_id': 1, 'weight': 3, 'region': 4, 'delivery_hours': ["00:59-23:59"]}]}
        self.data_validator.validate_orders(orders_data)
        delivery_hours = orders_data['data'][0]['delivery_hours']
        self.assertIsInstance(delivery_hours, list)
        self.assertEqual(len(delivery_hours), 1)
        self.assertIsInstance(delivery_hours[0], tuple)
        begin_time, end_time = delivery_hours[0]
        self.assertEqual(begin_time, datetime.strptime("00:59", "%H:%M"))
        self.assertEqual(end_time, datetime.strptime("23:59", "%H:%M"))

    @parameterized.expand([
        [{'data': [{'order_id': 1, 'weight': 3, 'region': 4, 'delivery_hours': ["09:59-33:33"]}]}],
        [{'data': [{'order_id': 1, 'weight': 3, 'region': 4, 'delivery_hours': ["9:9-22:33"]}]}]

    ])
    def test_orders_should_be_incorrect_when_delivery_hours_in_wrong_format(self, orders_data: dict):
        self.assert_exception(orders_data, "{'orders': [{'id': 1}]}")

    @parameterized.expand([
        [{'data': [{'order_id': 1, 'weight': 0, 'region': 4, 'delivery_hours': ["00:59-23:59"]}]}],
        [{'data': [{'order_id': 1, 'weight': 51, 'region': 4, 'delivery_hours': ["00:59-23:59"]}]}]
    ])
    def test_orders_weight_should_have_in_correct_interval(self, orders_data: dict):
        self.assert_exception(orders_data, "{'orders': [{'id': 1}]}")


if __name__ == '__main__':
    unittest.main()
