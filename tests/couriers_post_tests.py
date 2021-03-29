import unittest
from unittest.mock import MagicMock

from bson import json_util
from jsonschema import ValidationError
from tests import test_utils


class CouriersPostTests(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.app, cls.db, cls.validator = test_utils.set_up_service()

    def test_successful_couriers_post_should_return_list_ids(self):
        headers = [('Content-Type', 'application/json')]
        couriers_data = test_utils.read_data('couriers.json')

        http_response = self.app.post('/couriers', data=json_util.dumps(couriers_data), headers=headers)

        response_data = http_response.get_json()

        couriers_list = []
        for courier in couriers_data['data']:
            couriers_list.append({'id': courier['courier_id']})

        self.assertEqual(http_response.status_code, 201)
        self.assertEqual({'couriers': couriers_list}, response_data)

    def test_when_no_content_type_should_return_bad_request(self):
        http_response = self.app.post('/couriers', data=json_util.dumps({'test': 1}))

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Content-Type must be application/json', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_database_error_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]
        couriers_data = {'data': [{'courier_id': '5a8f1e368f7936badfbb0cfa',
                                   'courier_type': 'bike', 'regions': [], 'working_hours': []}] }

        self.app.post('/couriers', data=json_util.dumps(couriers_data), headers=headers)
        http_response = self.app.post('/couriers', data=json_util.dumps(couriers_data), headers=headers)

        http_data = http_response.get_data(as_text=True)
        self.assertIn('Database error: ', http_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_incorrect_json_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]

        http_response = self.app.post('/couriers', data='{', headers=headers)

        response_data = http_response.get_data(as_text=True)
        self.assertIn('Error when parsing JSON: ', response_data)
        self.assertEqual(400, http_response.status_code)

    def test_when_invalid_couriers_should_return_bad_request(self):
        headers = [('Content-Type', 'application/json')]
        mock_validation = MagicMock(side_effect=ValidationError('message'))
        with unittest.mock.patch.object(self.validator, 'validate_couriers', mock_validation):
            req = {'data': [{'courier_id': 1, 'courier_type': 'bike', 'regions': [], 'working_hours': []},
                            {'courier_id': 1, 'courier_type': 'bike', 'regions': [], 'working_hours': []}]}

            http_response = self.app.post('/couriers', data=json_util.dumps(req), headers=headers)

            response_data = http_response.get_json()  # TODO have problems

            self.assertEqual(1, response_data['validation_error']['couriers'][0]['id'])
            self.assertEqual(2, response_data['validation_error']['couriers'][1]['id'])
            self.assertEqual(400, http_response.status_code)


if __name__ == '__main__':
    unittest.main()
