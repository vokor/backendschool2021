import logging
from collections import defaultdict
from datetime import datetime
from multiprocessing import Lock

from flask import Flask, request
from pymongo import ReturnDocument
from pymongo.database import Database
from pymongo.errors import PyMongoError
from pymongo.results import InsertOneResult
from werkzeug.exceptions import BadRequest

from application.data_validator import DataValidator
from application.exception_handler import handle_exceptions
from utils.preparer import prepare_couriers, prepare_orders
from utils.utils import split_orders

logger = logging.getLogger(__name__)


def make_app(db: Database, data_validator: DataValidator) -> Flask:
    app = Flask(__name__)

    locks = defaultdict(Lock)

    @app.route('/couriers', methods=['POST'])
    @handle_exceptions(logger)
    def add_couriers():

        if not request.is_json:
            return BadRequest('Content-Type must be application/json')

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
                return PyMongoError('Operation was not acknowledged')

    @app.route('/couriers/<int:courier_id>', methods=['PATCH'])
    @handle_exceptions(logger)
    def patch_courier(courier_id):
        if not request.is_json:
            return BadRequest('Content-Type must be application/json')

        patch_data = request.get_json()
        data_validator.validate_courier_patch(patch_data)

        update_data = {
            '$set': {f'$.{key}': val for key, val in patch_data.items()}
        }
        courier: dict = db['couriers'].find_one_and_update(
            filter={'_id': courier_id}, update=update_data, return_document=ReturnDocument.AFTER)
        if courier is None:
            return PyMongoError('Courier with specified id not found')

        assigned_orders = {
            'status': 'in_progress',
            'courier_id': courier_id
        }
        list_orders = list(db['orders'].find(filter=assigned_orders))
        if len(list_orders) == 0:
            return courier, 201
        av_orders, un_orders = split_orders(list_orders, courier['working_hours'])
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
        db['orders'].update_many(
            filter={'_id': {'$in': un_orders}}, update=update_data)  # TODO: remove duplicate code

        return courier, 201

    @app.route('/orders', methods=['POST'])
    @handle_exceptions(logger)
    def add_orders():

        if not request.is_json:
            return BadRequest('Content-Type must be application/json')

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
                return PyMongoError('Operation was not acknowledged')

    @app.route('/orders/assign', methods=['POST'])
    @handle_exceptions(logger)
    def assign_orders():

        if not request.is_json:
            return BadRequest('Content-Type must be application/json')

        assign_id_data = request.get_json()
        data_validator.validate_orders(assign_id_data)

        courier = db['couriers'].find_one({'_id': assign_id_data['courier_id']})
        if courier is None:
            return PyMongoError('Courier with specified id not found')

        assigned_orders = {
            'status': 'in_progress',
            'courier_id': courier['_id']
        }
        list_orders = list(db['orders'].find(filter=assigned_orders))
        if len(list_orders):
            assign_time = list_orders[0]['assign_time']
        else:
            max_weight = 10 * (courier['courier_type'] == 'foot') + 15 * (
                    courier['courier_type'] == 'bike') + 50 * (courier['courier_type'] == 'car')
            matching_orders = {
                'status': 'not_assigned',
                'weight': {'$lte': max_weight},
                'region': {'$in': courier['regions']},
            }
            list_orders = list(db['orders'].find(filter=matching_orders))
            if len(list_orders) == 0:
                return {'orders': []}, 201
            av_orders, _ = split_orders(list_orders, courier['working_hours'])
            if len(av_orders) == 0:
                return {'orders': []}, 201
            else:
                assign_time = datetime.utcnow().isoformat("T") + "Z"  # <-- get time in UTC
                update_data = {
                    '$set': {
                        'courier_id': courier['_id'],
                        'status': 'in_progress',
                        'assign_time': assign_time,
                    }
                }
                av_order_ids = list(map(lambda x: x['_id'], av_orders))
                db['orders'].update_many(
                    filter={'_id': {'$in': av_order_ids}}, update=update_data)
                list_orders = list(db['orders'].find(filter=assigned_orders))
        orders_id = []
        for order in list_orders:
            orders_id.append({'id': order['_id']})
        response = {'orders': orders_id, 'assign_time': assign_time}
        return response, 201

    @app.route('/orders/complete', methods=['POST'])
    @handle_exceptions(logger)
    def complete_order():

        if not request.is_json:
            return BadRequest('Content-Type must be application/json')

        complete_data = request.get_json()
        data_validator.validate_orders(complete_data)  # TODO: validation settings

        if db['couriers'].find_one({'_id': complete_data['courier_id']}) is None:
            return PyMongoError('Courier with specified id not found')

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
            filter_data = {
                '_id': complete_data['order_id'],
                'courier_id': complete_data['courier_id'],
                'status': 'completed'
            }
            db_response: dict = db['orders'].find_one(filter=filter_data)
            if db_response is None:
                return PyMongoError('Order with specified id not found')
            return {'order_id': db_response['_id']}, 201
        orders_count = db['orders'].find({'courier_id': complete_data['courier_id'],
                                          'status': 'in_progress'}).count()
        if orders_count == 0:
            db_response_courier: dict = db['couriers'].find_one_and_update(
                filter={'_id': complete_data['courier_id']},
                update={'$inc': {'assigns': 1}}, return_document=ReturnDocument.AFTER)
            if db_response_courier is None:
                return PyMongoError('Courier with specified id not found')

        return {'order_id': db_response['_id']}, 201

    return app
