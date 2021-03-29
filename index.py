import os

from application.custom_mongo_client import CustomMongoClient
from application.data_validator import DataValidator
from application.service import make_app

db_uri = os.environ['DATABASE_URI']
db_name = os.environ['DATABASE_NAME']
replica_set = os.environ['REPLICA_SET']

client = CustomMongoClient(db_uri, 27017, replica_set)
db = client[db_name]
data_validator = DataValidator()
app = make_app(db, data_validator)

if __name__ == '__main__':
    app.run()
