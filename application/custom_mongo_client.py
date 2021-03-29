import logging

from mongomock import MongoClient
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)


def _initiate_replica_set(host: str, port: int):
    """Инициализирует replica set через новое подключение к узлу монго.

    :param str host: адрес узла монго, на котором инициируется replica set
    :param int port: порт, который прослушивает узел монго
    """
    client = MongoClient(host, port)
    try:
        client.admin.command('replSetInitiate')
    except PyMongoError:
        logger.info('Replica set already initiated')
    finally:
        client.close()


class CustomMongoClient(MongoClient):
    """Класс для подключения к базе данных монго и автоматической инициализации replica set."""

    def __init__(self, host: str, port: int, replica_set: str):
        super().__init__(host, port, replicaset=replica_set)
        _initiate_replica_set(host, port)
