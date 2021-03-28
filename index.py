import configparser
import logging
import os
from collections import defaultdict
from datetime import datetime
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
from preparer import prepare_couriers, prepare_orders
from utils import split_orders


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
            data_to_insert = prepare_couriers(couriers_data)

            with locks['post_couriers']:
                db_response: InsertOneResult = db['couriers'].insert_many(
                    data_to_insert)  # TODO: catch human readable exception

                if db_response.acknowledged:
                    couriers_list = []
                    for courier in couriers_data['data']:
                        couriers_list.append({'id': courier['courier_id']})
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

    @app.route('/couriers/<int:courier_id>', methods=['PATCH'])
    def patch_courier(courier_id):
        if not request.is_json:
            return make_error_response('Content-Type must be application/json', 400)

        try:
            patch_data = request.get_json()
            data_validator.validate_courier_patch(patch_data)

            update_data = {
                '$set': {f'$.{key}': val for key, val in patch_data.items()}
            }
            courier: dict = db['couriers'].find_one_and_update(
                filter={'_id': courier_id}, update=update_data, return_document=ReturnDocument.AFTER)
            if courier is None:
                return make_error_response('Courier with specified id not found', 400)

            assigned_orders = {
                'status': 'in_progress',
                'courier_id': courier_id
            }
            db_response: dict = db['orders'].find(filter=assigned_orders)
            if db_response in None:
                return courier, 201
            av_orders, un_orders = split_orders(db_response, courier['working_hours'])
            max_weight = 10 * (courier['courier_type'] == 'foot') + 15 * (
                    courier['courier_type'] == 'bike') + 50 * (courier['courier_type'] == 'car')
            for order in av_orders:
                if order['weight'] > max_weight or order['region'] not in courier['regions']:
                    un_orders.append(order['_id'])
            update_data = {
                'status': 'not_assigned',
                'assign_time': None,
                'courier_id': None
            }
            db['orders'].find_one_and_update(
                filter={'_id': {'$in': un_orders}}, update=update_data,
                return_document=ReturnDocument.AFTER)  # TODO: remove duplicate code

            return courier, 201
        except ValidationError as e:
            return make_error_response('Courier patch is not valid: ' + str(e), 400)
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
            data_to_insert = prepare_orders(orders_data)

            with locks['post_orders']:

                db_response: InsertOneResult = db['orders'].insert_many(
                    data_to_insert)  # TODO: catch human readable exception

                if db_response.acknowledged:
                    orders_list = []
                    for order in orders_data['data']:
                        orders_list.append({'id': order['order_id']})
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

    @app.route('/orders/assign', methods=['POST'])
    def assign_orders():

        if not request.is_json:
            return make_error_response('Content-Type must be application/json', 400)

        try:
            assign_id_data = request.get_json()
            data_validator.validate_orders(assign_id_data)

            courier = db['couriers'].find({'_id': assign_id_data['courier_id']})
            if courier is None:
                return make_error_response('Courier with specified id not found', 400)

            assigned_orders = {
                'status': 'in_progress',
                'courier_id': courier.courier_id
            }
            db_response: dict = db['orders'].find(filter=assigned_orders)
            assign_time = db_response[0]['assign_time']
            if db_response is None:
                max_weight = 10 * (courier['courier_type'] == 'foot') + 15 * (
                        courier['courier_type'] == 'bike') + 50 * (courier['courier_type'] == 'car')
                matching_orders = {
                    'status': 'not_assigned',
                    'weight': {'$lte': max_weight},
                    'region': {'$in': courier.regions},
                }
                db_response: dict = db['orders'].find(filter=matching_orders)
                if db_response is None:
                    return {'orders': []}, 201
                av_orders, _ = split_orders(db_response, courier['working_hours'])
                if len(av_orders) == 0:
                    return {'orders': []}, 201
                else:
                    assign_time = datetime.now(tz=None)
                    update_data = {
                        '$set': {
                            'status': 'in_progress',
                            'assign_time': assign_time,
                            'courier_id': courier.courier_id
                        }
                    }
                    av_order_ids = list(map(lambda x: x['_id'], av_orders))
                    db_response: dict = db['orders'].find_one_and_update(
                        filter={'_id': {'$in': av_order_ids}},
                        update=update_data, return_document=ReturnDocument.AFTER)
            orders_id = []
            for order in db_response:
                orders_id.append({'id': order['_id']})
            response = {'orders': orders_id, 'assign_time': assign_time}
            return response, 201

        except ValidationError as e:
            response = {'validation_error': e.message}
            return response, 400
        except BadRequest as e:
            return make_error_response('Error when parsing JSON: ' + str(e), 400)
        except PyMongoError as e:
            return make_error_response('Database error: ' + str(e), 400)
        except Exception as e:
            return make_error_response(str(e), 400)

    @app.route('/orders/complete', methods=['POST'])
    def complete_order():

        if not request.is_json:
            return make_error_response('Content-Type must be application/json', 400)

        try:
            complete_data = request.get_json()
            data_validator.validate_orders(complete_data)  # TODO: validation settings

            if db['couriers'].find_one({'_id': complete_data['courier_id']}) is None:
                return make_error_response('Courier with specified id not found', 400)

            if db['orders'].find({'_id': complete_data['order_id'], 'status': 'completed'}) is None:
                return complete_data['order_id'], 201
            update_data = {
                '$set': {
                    'complete_time': complete_data['complete_time'],
                    'status': 'completed'
                }
            }
            filter_data = {
                '_id': complete_data['order_id'],
                'courier_id': complete_data['courier_id'],
                'status': 'in_progress'
            }
            db_response: dict = db['orders'].find_one_and_update(
                filter=filter_data, update=update_data, return_document=ReturnDocument.AFTER)
            if db_response is None:
                return make_error_response('Order with specified id not found', 400)
            orders_count = db['orders'].find({'courier_id': complete_data['courier_id'],
                                              'status': 'in_progress'}).count()
            if orders_count == 0:
                db_response_courier: dict = db['couriers'].find_one_and_update(
                    filter={'_id': complete_data['courier_id']},
                    update={'$inc': {'assigns': 1}}, return_document=ReturnDocument.AFTER)
                if db_response_courier is None:
                    return make_error_response('Courier with specified id not found', 400)

            return {'order_id': db_response['_id']}, 201
        except ValidationError as e:
            response = {'validation_error': e.message}
            return response, 400
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
