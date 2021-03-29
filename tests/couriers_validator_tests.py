import logging
import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock

from bson import json_util
from jsonschema import ValidationError
from parameterized import parameterized

from data_validator import DataValidator
from tests.test_utils import read_data


class CouriersValidatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_validator = DataValidator()
        logging.disable(logging.CRITICAL)

    def test_correct_couriers_should_be_valid(self):
        couriers_data = read_data('couriers.json')
        self.data_validator.validate_couriers(couriers_data)

    def assert_exception(self, couriers_data: dict, expected_exception_message: str):
        with self.assertRaises(ValidationError) as context:
            self.data_validator.validate_couriers(couriers_data)
        self.assertIn(expected_exception_message, str(context.exception.message))

    @parameterized.expand([({}, 'data')])
    def test_couriers_should_be_incorrect_when_missing_data_field(self, couriers_data: dict, field_name: str):
        self.assert_exception(couriers_data, f'\'{field_name}\' is a required property')

    @parameterized.expand([
        [{'data': [{'courier_id': 1, 'regions': [], 'working_hours': []}]}],
        [{'data': [{'courier_id': 1, 'courier_type': 'bike', 'working_hours': []}]}],
        [{'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': []}]}]
    ])
    def test_couriers_should_be_incorrect_when_missing_field(self, couriers_data: dict):
        self.assert_exception(couriers_data, "{'couriers': [{'id': 1}]}")

    @parameterized.expand([
        ({'data': None}, 'array'),
        ({'data': ['']}, 'object'),
    ])
    def test_couriers_should_be_incorrect_when_wrong_type_of_field(self, couriers_data: dict, data_type: str):
        self.assert_exception(couriers_data, f'is not of type \'{data_type}\'')

    @parameterized.expand([
        [{'data': [{'courier_id': 1, 'courier_type': None, 'regions': [], 'working_hours': []}]}],
        [{'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': None, 'working_hours': []}]}],
        [{'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [], 'working_hours': None}]}],
        [{'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [''], 'working_hours': []}]}]
    ])
    def test_couriers_should_be_incorrect_when_wrong_type_of_field(self, couriers_data: dict):
        self.assert_exception(couriers_data, "{'couriers': [{'id': 1}]}")

    def test_couriers_data_should_be_correct_with_different_field_order(self):
        couriers_data = {'data': [{'working_hours': [], 'courier_type': 'bike', 'regions': [], 'courier_id': 1}]}
        self.data_validator.validate_couriers(couriers_data)

    @parameterized.expand([
        ({'EXTRA': 0, 'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [],
                                'working_hours': ["00:59-23:59"]}]}, ''),
        ({'data': [{'EXTRA': 0, 'courier_id': 1, 'courier_type': 'bike', 'regions': [],
                    'working_hours': ["00:59-23:59"]}]}, "{'couriers': [{'id': 1}]}"),
    ])
    def test_couriers_should_be_incorrect_when_containing_extra_fields(self, couriers_data: dict, field_name: str):
        self.assert_exception(couriers_data, field_name)

    @unittest.mock.patch('jsonschema.validate')
    def test_couriers_should_be_incorrect_when_courier_ids_not_unique(self, _):
        couriers_data = {'data': [{'courier_id': 1}, {'courier_id': 1}]}
        self.assert_exception(couriers_data, 'Couriers ids are not unique')

    @unittest.mock.patch('jsonschema.validate')
    def test_correct_working_hours_should_be_parsed(self, _):
        couriers_data = {
            'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [], 'working_hours': ["00:59-23:59"]}]}
        self.data_validator.validate_couriers(couriers_data)
        working_hours = couriers_data['data'][0]['working_hours']
        self.assertIsInstance(working_hours, list)
        self.assertEqual(len(working_hours), 1)
        self.assertIsInstance(working_hours[0], tuple)
        begin_time, end_time = working_hours[0]
        self.assertEqual(begin_time, datetime.strptime("00:59", "%H:%M"))
        self.assertEqual(end_time, datetime.strptime("23:59", "%H:%M"))

    @parameterized.expand([
        [{'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [],'working_hours': ["09:59-33:33"]}]}],
        [{'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [],'working_hours': ["9:9-22:33"]}]}]
    ])
    def test_couriers_should_be_incorrect_when_working_hours_in_wrong_format(self, couriers_data: dict):
        self.assert_exception(couriers_data, "{'couriers': [{'id': 1}]}")


if __name__ == '__main__':
    unittest.main()
