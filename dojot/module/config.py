"""
Configuration data module
"""

import os
import yaml

class Config:
    """
    Main configuration class
    This class contains all needed configuration for this library
    """
    def __init__(self, config=None):
        if config is not None:
            self.kafka = config["kafka"]
            self.data_broker = config["data_broker"]
            self.auth = config["auth"]
            self.dojot = config["dojot"]
        else:
            self.load_defaults()


    def load_defaults(self):
        """
        Load default configuration
        """
        self.kafka = {
            "producer": {
                "client.id": "kafka",
                "metadata.broker.list": os.environ.get('KAFKA_HOSTS', "kafka:9092"),
                "compression.codec": "gzip",
                "retry.backoff.ms": 200,
                "message.send.max.retries": 10,
                "socket.keepalive.enable": True,
                "queue.buffering.max.messages": 100000,
                "queue.buffering.max.ms": 1000,
                "batch.num.messages": 1000000,
                "dr_cb": True
            },
            "consumer": {
                "group.id": os.environ.get('KAFKA_GROUP_ID', "data-broker"),
                "metadata.broker.list": os.environ.get('KAFKA_HOSTS', "kafka:9092")
            }
        }
        self.data_broker = {
            "url": os.environ.get('DATA_BROKER_URL', "http://data-broker")
        }

        self.auth = {
            "url": os.environ.get('AUTH_URL', "http://auth:5000")
        }

        self.dojot = {
            "management_service": os.environ.get('DOJOT_SERVICE_MANAGEMENT', "dojot-management"),
            "subjects": {
                "tenancy": os.environ.get('DOJOT_SUBJECT_TENANCY', "dojot.tenancy"),
                "devices": os.environ.get('DOJOT_SUBJECT_DEVICES', "dojot.device-manager.device"),
                "device_data": os.environ.get('DOJOT_SUBJECT_DEVICE_DATA', "device-data")
            }
        }

        self.load_env()

    def load_file(self, config_file):
        """
        Load configuration from a file
        """
        parsed_config = yaml.safe_load(open(config_file))
        self.kafka = parsed_config["kafka"]
        self.data_broker = parsed_config["data_broker"]
        self.auth = parsed_config["auth"]
        self.dojot = parsed_config["dojot"]

        self.load_env()

    def load_env(self):
        """
        Load configuration from environment variables
        """
        self.kafka["producer"]["metadata.broker.list"] = os.environ.get(
            'KAFKA_HOSTS', self.kafka["producer"]["metadata.broker.list"])

        self.kafka["consumer"]["metadata.broker.list"] = os.environ.get(
            'KAFKA_HOSTS', self.kafka["consumer"]["metadata.broker.list"])

        self.kafka["consumer"]["group.id"] = os.environ.get(
            'KAFKA_GROUP_ID', self.kafka["consumer"]["group.id"])

        self.data_broker["url"] = os.environ.get(
            'DATA_BROKER_URL', self.data_broker["url"])

        self.auth["url"] = os.environ.get('AUTH_URL', self.auth["url"])

        self.dojot["management_service"] = os.environ.get(
            'DOJOT_SERVICE_MANAGEMENT', self.dojot["management_service"])

        self.dojot["subjects"]["tenancy"] = os.environ.get(
            'DOJOT_SUBJECT_TENANCY', self.dojot["subjects"]["tenancy"])

        self.dojot["subjects"]["devices"] = os.environ.get(
            'DOJOT_SUBJECT_DEVICES', self.dojot["subjects"]["devices"])

        self.dojot["subjects"]["device_data"] = os.environ.get(
            'DOJOT_SUBJECT_DEVICE_DATA', self.dojot["subjects"]["device_data"])
