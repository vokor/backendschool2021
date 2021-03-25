import configparser
from flask import Flask, request
from pymongo import MongoClient


def make_app(db: MongoClient) -> Flask:
    app = Flask(__name__)

    @app.route('/couriers', methods=['POST'])
    def add_couriers():
        if request.content_type != 'application/json':
            return 'Content-Type must be application/json', 400

        couriers_data = request.get_json()

        db_response = db['couriers'].insert_one(couriers_data)

        couriers_list = []
        for courier in couriers_data['data']:
            couriers_list.append({'id': courier['courier_id']})

        if db_response.acknowledged:
            response = {"couriers": couriers_list}
            return response, 201
        return 400

    return app


def main():
    config = configparser.ConfigParser()
    config.read('config.cfg')
    db_uri = config['DATABASE']['DATABASE_URI']
    db_name = config['DATABASE']['DATABASE_NAME']

    db = MongoClient(db_uri)[db_name]
    app = make_app(db)
    app.run(host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
