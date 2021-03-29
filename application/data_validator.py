import os

import jsonschema
from bson import json_util
from jsonschema import ValidationError
import iso8601

from utils.parser import parse_hours


class DataValidator(object):
    def __init__(self):
        self.data_schema = self.__load_schema('data_schema.json')
        self.courier_schema = self.__load_schema('courier_schema.json')
        self.order_schema = self.__load_schema('order_schema.json')
        self.complete_schema = self.__load_schema('complete_schema.json')
        self.assign_schema = self.__load_schema('assign_schema.json')
        self.courier_patch_schema = self.__load_schema('courier_patch_schema.json')

    @staticmethod
    def __load_schema(schema_name: str):
        with open(os.path.join(os.path.dirname(__file__), 'schemas', schema_name)) as f:
            return json_util.loads(f.read())

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

    def validate_orders(self, orders_data: dict):
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
        jsonschema.validate(patch_data, self.courier_patch_schema)
