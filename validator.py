import os

import jsonschema
from bson import json_util


class Validator:
    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__), 'schemas', 'couriers_schema.json')) as f:
            self.couriers_schema = json_util.loads(f.read())

    def validate_couriers(self, couriers_data: dict):
        jsonschema.validate(couriers_data, self.couriers_schema)
