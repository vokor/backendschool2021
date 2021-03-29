import logging
import unittest
from unittest.mock import MagicMock

from bson import json_util
from jsonschema import ValidationError

import tests.test_utils as test_utils
from preparer import prepare_couriers


class CourierPatchTests(unittest.TestCase): # TODO: add tests
    @classmethod
    def setUp(cls):
        cls.app, cls.db, cls.validator = test_utils.set_up_service()
        couriers_data = test_utils.read_data('couriers.json')
        data_to_insert = prepare_couriers(couriers_data)
        cls.db['couriers'].insert_many(data_to_insert)

    def test_update_db_when_patch_received(self):
        headers = [('Content-Type', 'application/json')]
        patch_data = {'regions': [11, 44, 55]}

        http_response = self.app.patch('/couriers/2', data=json_util.dumps(patch_data), headers=headers)

        response_data = http_response.get_json()
        self.assertEqual(http_response.status_code, 201)
        self.assertEqual(patch_data['regions'], response_data['regions'])

    def test_should_return_bad_request_when_no_content_type(self):
        patch_data = {'regions': [11, 33, 2]}

        http_response = self.app.patch('/couriers/2', data=json_util.dumps(patch_data))

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Content-Type must be application/json', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_should_return_bad_request_when_no_courier_found(self):
        headers = [('Content-Type', 'application/json')]
        patch_data = {'regions': [11, 33, 2]}

        http_response = self.app.patch('/couriers/5', data=json_util.dumps(patch_data), headers=headers)

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Courier with specified id not found', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_should_return_bad_request_when_patch_not_valid(self): # TODO: why
        headers = [('Content-Type', 'application/json')]
        patch_data = {'regions': [11, 33, 2]}
        mock_validation = MagicMock(side_effect=ValidationError('message'))

        with unittest.mock.patch.object(self.validator, 'validate_courier_patch', mock_validation):
            http_response = self.app.patch('/couriers/2', data=json_util.dumps(patch_data), headers=headers)

            response_data = http_response.get_data(as_text=True)
            self.assertIn('Courier patch is not valid', response_data)
            self.assertEqual(400, http_response.status_code)

    def test_should_return_bad_request_when_incorrect_json(self):
        headers = [('Content-Type', 'application/json')]

        http_response = self.app.patch('/couriers/2', data='{', headers=headers)

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Error when parsing JSON', response_data)
        self.assertEqual(400, http_response.status_code)


if __name__ == '__main__':
    unittest.main()
