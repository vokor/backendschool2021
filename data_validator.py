import os
from datetime import datetime, timedelta

import jsonschema
from bson import json_util
from jsonschema import ValidationError
import iso8601

from parser import parse_hours


class DataValidator(object):
    def __init__(self):
        data_schema_path = os.path.join(os.path.dirname(__file__), 'schemas', 'data_schema.json')
        courier_schema_path = os.path.join(os.path.dirname(__file__), 'schemas', 'courier_schema.json')
        order_schema_path = os.path.join(os.path.dirname(__file__), 'schemas', 'order_schema.json')
        complete_schema_path = os.path.join(os.path.dirname(__file__), 'schemas', 'complete_schema.json')
        assign_schema_path = os.path.join(os.path.dirname(__file__), 'schemas', 'assign_schema.json')
        with open(data_schema_path) as f:
            self.data_schema = json_util.loads(f.read())
        with open(courier_schema_path) as f:
            self.courier_schema = json_util.loads(f.read())
        with open(order_schema_path) as f:
            self.order_schema = json_util.loads(f.read())
        with open(complete_schema_path) as f:
            self.complete_schema = json_util.loads(f.read())
        with open(assign_schema_path) as f:
            self.assign_schema = json_util.loads(f.read())

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
        parse_hours(couriers_data, 'working_hours')

    def validate_orders(self, orders_data: dict):  # TODO: may be duplicate code AND check weight 0.043434...
        jsonschema.validate(orders_data, self.data_schema)
        errors = []
        for order in orders_data['data']:
            try:
                jsonschema.validate(order, self.order_schema)
            except ValidationError as e:
                errors.append({'id': order['order_id']})
        if errors:
            raise ValidationError({'orders': errors})

        order_ids = {order['order_id'] for order in orders_data['data']}
        if len(order_ids) != len(orders_data['data']):
            raise ValidationError('Orders ids are not unique')
        parse_hours(orders_data, 'delivery_hours')

    def validate_complete(self, complete_data: dict):
        jsonschema.validate(complete_data, self.complete_schema, format_checker=jsonschema.FormatChecker())
        complete_data['complete_time'] = iso8601.parse_date(complete_data['complete_time'])

    def validate_assign(self, assign_data: dict):
        jsonschema.validate(assign_data, self.assign_schema)

    def validate_courier_patch(self, patch_data: dict):
        pass
