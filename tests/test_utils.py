import os
from typing import Tuple
from unittest.mock import MagicMock

from bson import json_util
from flask import Flask
from mongomock import MongoClient

from data_validator import DataValidator
from index import make_app


def create_mock_validator() -> DataValidator:
    validator = DataValidator()
    validator.validate_couriers = MagicMock()
    return validator


def read_couriers_data():
    with open(os.path.join(os.path.dirname(__file__), 'couriers.json')) as f:
        couriers_data = json_util.loads(f.read())
    return couriers_data


def set_up_service() -> Tuple[Flask, MongoClient, DataValidator]:
    db = MongoClient()['db']
    validator = create_mock_validator()
    app = make_app(db, validator).test_client()
    return app, db, validator
