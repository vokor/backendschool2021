import os
from datetime import datetime

import jsonschema
from bson import json_util
from jsonschema import ValidationError


class DataValidator(object):
    def __init__(self):
        data_schema_path = os.path.join(os.path.dirname(__file__), 'schemas', 'data_schema.json')
        courier_schema_path = os.path.join(os.path.dirname(__file__), 'schemas', 'courier_schema.json')
        with open(data_schema_path) as f:
            self.data_schema = json_util.loads(f.read())
        with open(courier_schema_path) as f:
            self.courier_schema = json_util.loads(f.read())

    def validate_couriers(self, couriers_data: dict):
        jsonschema.validate(couriers_data, self.data_schema)
        errors = []
        for courier in couriers_data['data']:
            try:
                jsonschema.validate(courier, self.courier_schema)
            except ValidationError:
                errors.append({'id': courier['courier_id']})
        if errors:
            raise ValidationError({'couriers': errors})

        courier_ids = {courier['courier_id'] for courier in couriers_data['data']}
        if len(courier_ids) != len(couriers_data['data']):
            raise ValidationError('Couriers ids are not unique')

        for courier in couriers_data['data']:
            parsed_time_list = []
            for working_hour in courier['working_hours']:
                time_parts = working_hour.split('-')
                begin_time = datetime.strptime(time_parts[0], "%H:%M")
                end_time = datetime.strptime(time_parts[1], "%H:%M")
                parsed_time_list.append((begin_time, end_time))
            courier['working_hours'] = parsed_time_list
