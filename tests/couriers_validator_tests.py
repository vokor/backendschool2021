import os
import unittest

from bson import json_util
from jsonschema import ValidationError
from parameterized import parameterized

from validator import Validator


class ValidatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.validator = Validator()

    def test_correct_scheme_should_be_valid(self):
        with open(os.path.join(os.path.dirname(__file__), 'couriers.json')) as f:
            import_data = json_util.loads(f.read())
        self.validator.validate_couriers(import_data)

    @parameterized.expand([
        ({}, 'data'),
        ({'data': [{'courier_type': '', 'regions': [], 'working_hours': []}]}, 'courier_id'),
        ({'data': [{'courier_id': 0, 'regions': [], 'working_hours': []}]}, 'courier_type'),
        ({'data': [{'courier_id': 0, 'courier_type': '', 'working_hours': []}]}, 'regions'),
        ({'data': [{'courier_id': 0, 'courier_type': '', 'regions': []}]}, 'working_hours'),
    ])
    def test_import_should_be_incorrect_when_missing_field(self, couriers_data: dict, field_name: str):
        with self.assertRaises(ValidationError) as context:
            self.validator.validate_couriers(couriers_data)
        self.assertIn(f'\'{field_name}\' is a required property', str(context.exception))

    @parameterized.expand([
        ({'data': None}, 'array'),
        ({'data': ['']}, 'object'),
        ({'data': [{'courier_id': None, 'courier_type': '', 'regions': [], 'working_hours': []}]}, 'integer'),
        ({'data': [{'courier_id': 0, 'courier_type': None, 'regions': [], 'working_hours': []}]}, 'string'),
        ({'data': [{'courier_id': 0, 'courier_type': '', 'regions': None, 'working_hours': []}]}, 'array'),
        ({'data': [{'courier_id': 0, 'courier_type': '', 'regions': [], 'working_hours': None}]}, 'array'),
        ({'data': [{'courier_id': 0, 'courier_type': '', 'regions': [''], 'working_hours': []}]}, 'integer'),

    ])
    def test_import_should_be_incorrect_when_wrong_type_of_field(self, import_data: dict, data_type: str):
        with self.assertRaises(ValidationError) as context:
            self.validator.validate_couriers(import_data)
        self.assertIn(f'is not of type \'{data_type}\'', str(context.exception))


if __name__ == '__main__':
    unittest.main()
