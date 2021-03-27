import configparser
import logging
import os
from collections import defaultdict
from multiprocessing import Lock
from typing import Tuple

from flask import Flask, request
from jsonschema.exceptions import ValidationError
from pymongo import MongoClient, ReturnDocument
from pymongo.database import Database
from pymongo.errors import PyMongoError
from pymongo.results import InsertOneResult
from werkzeug.exceptions import BadRequest

from data_validator import DataValidator


def make_app(db: Database, data_validator: DataValidator) -> Flask:
    app = Flask(__name__)

    def make_error_response(message: str, status_code: int) -> Tuple[dict, int]:
        app.logger.error(message)
        return {'message': message}, status_code

    locks = defaultdict(Lock)

    @app.route('/couriers', methods=['POST'])
    def add_couriers():

        if not request.is_json:
            return make_error_response('Content-Type must be application/json', 400)

        try:
            couriers_data = request.get_json()
            data_validator.validate_couriers(couriers_data)

            with locks['post_couriers']:
                # TODO: check for duplicate values in db
                db_response: InsertOneResult = db['couriers'].insert_one(couriers_data)
                couriers_list = []
                for courier in couriers_data['data']:
                    couriers_list.append({'id': courier['courier_id']})

                if db_response.acknowledged:
                    response = {'couriers': couriers_list}
                    return response, 201
                else:
                    return make_error_response('Operation was not acknowledged', 400)
        except ValidationError as e:
            response = {'validation_error': e.message}
            return response, 400
        except BadRequest as e:
            return make_error_response('Error when parsing JSON: ' + str(e), 400)
        except PyMongoError as e:
            return make_error_response('Database error: ' + str(e), 400)
        except Exception as e:
            return make_error_response(str(e), 400)

    @app.route('/orders', methods=['POST'])
    def add_orders():

        if not request.is_json:
            return make_error_response('Content-Type must be application/json', 400)

        try:
            orders_data = request.get_json()
            data_validator.validate_orders(orders_data)

            with locks['post_orders']:
                # TODO: check for duplicate values in db
                db_response: InsertOneResult = db['orders'].insert_one(orders_data)
                orders_list = []
                for order in orders_data['data']:
                    orders_list.append({'id': order['order_id']})

                if db_response.acknowledged:
                    response = {'orders': orders_list}
                    return response, 201
                else:
                    return make_error_response('Operation was not acknowledged', 400)
        except ValidationError as e:
            response = {'validation_error': e.message}
            return response, 400
        except BadRequest as e:
            return make_error_response('Error when parsing JSON: ' + str(e), 400)
        except PyMongoError as e:
            return make_error_response('Database error: ' + str(e), 400)
        except Exception as e:
            return make_error_response(str(e), 400)

    @app.route('/couriers/<int:courier_id>', methods=['PATCH'])
    def patch_courier(courier_id):
        if not request.is_json:
            return make_error_response('Content-Type must be application/json', 400)

        try:
            patch_data = request.get_json()
            data_validator.validate_courier_patch(patch_data)

            update_data = {
                '$set': {f'data.$.{key}': val for key, val in patch_data.items()}
            }
            projection = {
                '_id': 0,
                'data': {
                    '$elemMatch': {'courier_id': courier_id}
                }
            }
            db_response: dict = db['couriers'].find_one_and_update(
                filter={'data.courier_id': courier_id}, update=update_data,
                projection=projection, return_document=ReturnDocument.AFTER)
            if db_response is None:
                return make_error_response('Courier with specified id not found', 400)

            return {'data': db_response['data'][0]}, 201
        except ValidationError as e:
            return make_error_response('Courier patch is not valid: ' + str(e), 400)
        except BadRequest as e:
            return make_error_response('Error when parsing JSON: ' + str(e), 400)
        except PyMongoError as e:
            return make_error_response('Database error: ' + str(e), 400)
        except Exception as e:
            return make_error_response(str(e), 400)

    return app


def main():
    logging.basicConfig(filename='logs/service.log')

    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.cfg')
    if not os.path.exists(config_path):
        raise FileNotFoundError(config_path)
    config.read(config_path)
    db_uri = config['DATABASE']['DATABASE_URI']
    db_name = config['DATABASE']['DATABASE_NAME']

    db = MongoClient(db_uri)[db_name]
    data_validator = DataValidator()
    app = make_app(db, data_validator)
    app.run(host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
