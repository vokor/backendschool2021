import configparser
import os

from flask import Flask, request, jsonify
from jsonschema.exceptions import ValidationError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from werkzeug.exceptions import BadRequest

from validator import Validator


def make_app(db: MongoClient, validator: Validator) -> Flask:
    app = Flask(__name__)

    def make_message_response(message: str) -> dict:
        return {'message': message}

    @app.route('/couriers', methods=['POST'])
    def add_couriers():
        if not request.is_json:
            return make_message_response('Content-Type must be application/json'), 400

        try:
            couriers_data = request.get_json()
            validator.validate_couriers(couriers_data)

            db_response = db['couriers'].insert_one(couriers_data)

            couriers_list = []
            for courier in couriers_data['data']:
                couriers_list.append({'id': courier['courier_id']})

            if db_response.acknowledged:
                response = {"couriers": couriers_list}
                return response, 201
            else:
                return make_message_response('Operation was not acknowledged'), 400
        except ValidationError as e:
            response = {"validation_error": e.message}
            return jsonify(response), 400
        except BadRequest as e:
            return make_message_response('Error when parsing JSON: ' + str(e)), 400
        except PyMongoError as e:
            return make_message_response('Database error: ' + str(e)), 400
        except Exception as e:
            return make_message_response(str(e)), 400

    return app


def main():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.cfg')
    if not os.path.exists(config_path):
        raise FileNotFoundError(config_path)
    config.read(config_path)
    db_uri = config['DATABASE']['DATABASE_URI']
    db_name = config['DATABASE']['DATABASE_NAME']

    db = MongoClient(db_uri)[db_name]
    validator = Validator()
    app = make_app(db, validator)
    app.run(host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
