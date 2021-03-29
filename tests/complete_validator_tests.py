import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock

from iso8601 import iso8601
from jsonschema import ValidationError
from parameterized import parameterized

from data_validator import DataValidator


class CompleteValidatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_validator = DataValidator()

    def test_correct_orders_complete_should_be_valid(self):
        complete_data = {'courier_id': 2, 'order_id': 33, 'complete_time': '2021-01-10T10:33:01.42Z'}
        self.data_validator.validate_complete(complete_data)

    def assert_exception(self, complete_data: dict, expected_exception_message: str):
        with self.assertRaises(ValidationError) as context:
            self.data_validator.validate_complete(complete_data)
        self.assertIn(expected_exception_message, str(context.exception.message))

    @parameterized.expand([
        ({'order_id': 33, 'complete_time': '2021-01-10T10:33:01.42Z'}, 'courier_id'),
        ({'courier_id': 2, 'complete_time': '2021-01-10T10:33:01.42Z'}, 'order_id'),
        ({'courier_id': 2, 'order_id': 33}, 'complete_time')
    ])
    def test_complete_should_be_incorrect_when_missing_field(self, import_data: dict, field_name: str):
        self.assert_exception(import_data, f'\'{field_name}\' is a required property')

    @parameterized.expand([
        ({'courier_id': '', 'order_id': 33, 'complete_time': '2021-01-10T10:33:01.42Z'}, 'integer'),
        ({'courier_id': 2, 'order_id': '', 'complete_time': '2021-01-10T10:33:01.42Z'}, 'integer'),
        ({'courier_id': 2, 'order_id': 33, 'complete_time': 'abc'}, 'date-time'),
    ])
    def test_complete_should_be_incorrect_when_wrong_type_of_field(self, import_data: dict, data_type: str):
        self.assert_exception(import_data, f'\'{data_type}\'')

    def test_complete_should_be_correct_with_different_field_order(self):
        complete_data = {'complete_time': '2021-01-10T10:33:01.42Z', 'order_id': 33, 'courier_id': 2}
        self.data_validator.validate_complete(complete_data)

    def test_complete_should_be_incorrect_when_containing_extra_fields(self):
        complete_data = {'EXTRA': 0, 'complete_time': '2021-01-10T10:33:01.42Z', 'order_id': 33, 'courier_id': 2}
        self.assert_exception(complete_data, '')

    @unittest.mock.patch('jsonschema.validate')
    def test_correct_date_time_should_be_parsed(self, _):
        complete_data = {'courier_id': 2, 'order_id': 33, 'complete_time': '2021-01-10T10:33:01.42Z'}
        self.data_validator.validate_complete(complete_data)
        date_time: datetime = complete_data['complete_time']
        self.assertIsInstance(date_time, datetime)
        self.assertEqual(iso8601.parse_date('2021-01-10T10:33:01.42Z'), date_time)


if __name__ == '__main__':
    unittest.main()
