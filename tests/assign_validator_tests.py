import logging
import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock

from iso8601 import iso8601
from jsonschema import ValidationError
from parameterized import parameterized

from data_validator import DataValidator


class AssignValidatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_validator = DataValidator()
        logging.disable(logging.CRITICAL)

    def test_correct_assign_orders_should_be_valid(self):
        assign_data = {'courier_id': 2}
        self.data_validator.validate_assign(assign_data)

    def assert_exception(self, assign_data: dict, expected_exception_message: str):
        with self.assertRaises(ValidationError) as context:
            self.data_validator.validate_assign(assign_data)
        self.assertIn(expected_exception_message, str(context.exception.message))

    def test_assign_should_be_incorrect_when_missing_field(self):
        self.assert_exception({}, '\'courier_id\' is a required property')

    def test_assign_should_be_incorrect_when_extra_field(self):
        self.assert_exception({'EXTRA': 0, 'courier_id': 2}, '')

    def test_assign_should_be_incorrect_when_wrong_type_of_field(self):
        self.assert_exception({'courier_id': ''}, '\'integer\'')


if __name__ == '__main__':
    unittest.main()
