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
    validator.validate_orders = MagicMock()
    return validator


def read_data(filename: str) -> dict:
    with open(os.path.join(os.path.dirname(__file__), 'json', filename)) as f:
        import_data = json_util.loads(f.read())
    return import_data


def set_up_service() -> Tuple[Flask, MongoClient, DataValidator]:
    db = MongoClient()['db']
    validator = create_mock_validator()
    app = make_app(db, validator).test_client()
    return app, db, validator
