import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock

from bson import json_util
from jsonschema import ValidationError
from parameterized import parameterized

from data_validator import DataValidator


class CouriersValidatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_validator = DataValidator()

    def test_correct_couriers_should_be_valid(self):
        with open(os.path.join(os.path.dirname(__file__), 'couriers.json')) as f:
            couriers_data = json_util.loads(f.read())
        self.data_validator.validate_couriers(couriers_data)

    def assert_exception(self, couriers_data: dict, expected_exception_message: str):
        with self.assertRaises(ValidationError) as context:
            self.data_validator.validate_couriers(couriers_data)
        self.assertIn(expected_exception_message, str(context.exception.message))

    @parameterized.expand([({}, 'data')])
    def test_couriers_should_be_incorrect_when_missing_data_field(self, couriers_data: dict, field_name: str):
        self.assert_exception(couriers_data, f'\'{field_name}\' is a required property')

    @parameterized.expand([
        ({'data': [{'courier_id': 1, 'regions': [], 'working_hours': []}]}, 'courier_type'),
        ({'data': [{'courier_id': 1, 'courier_type': 'bike', 'working_hours': []}]}, 'regions'),
        ({'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': []}]}, 'working_hours'),
    ])
    def test_couriers_should_be_incorrect_when_missing_field(self, couriers_data: dict, field_name: str):
        self.assert_exception(couriers_data, "{'couriers': [{'id': 1}]}")

    @parameterized.expand([
        ({'data': None}, 'array'),
        ({'data': ['']}, 'object'),
    ])
    def test_couriers_should_be_incorrect_when_wrong_type_of_field(self, couriers_data: dict, data_type: str):
        self.assert_exception(couriers_data, f'is not of type \'{data_type}\'')

    @parameterized.expand([
        ({'data': [{'courier_id': 1, 'courier_type': None, 'regions': [], 'working_hours': []}]}, 'string'),
        ({'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': None, 'working_hours': []}]}, 'array'),
        ({'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [], 'working_hours': None}]}, 'array'),
        ({'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [''], 'working_hours': []}]}, 'integer'),
    ])
    def test_couriers_should_be_incorrect_when_wrong_type_of_field(self, couriers_data: dict, data_type: str):
        self.assert_exception(couriers_data, "{'couriers': [{'id': 1}]}")

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
        ({'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [], 'working_hours': ["09:59-33:33"]}]}, 'hour'),
        ({'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [], 'working_hours': ["9:9-22:33"]}]}, 'hour')

    ])
    def test_couriers_should_be_incorrect_when_working_hours_in_wrong_format(self, couriers_data: dict, data_type: str):
        self.assert_exception(couriers_data, "{'couriers': [{'id': 1}]}")


if __name__ == '__main__':
    unittest.main()
