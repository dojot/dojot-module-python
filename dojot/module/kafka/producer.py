"""
Kafka producer
"""
import json
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from ..logger import Log

LOGGER = Log().color_log()

class Producer:
    """
    Kafka producer
    """
    def __init__(self, config):
        """
        Producer object initialization

        @type config: dojot.module.Config
        @param config: The configuration object
        """
        self.broker = config.kafka['producer']['metadata.broker.list'].split(',')
        self.producer = None

    def init(self):
        """
        Producer initialization

        This function will create the KafkaProducer.
        """
        try:
            self.producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                          bootstrap_servers=self.broker)
        except AssertionError as error:
            LOGGER.warning("Ignoring assertion error on kafka producer %s", error)
            return -1

        return 0

    def produce(self, topic, msg):
        """
        Produce a message to a Kafka topic

        @type topic: str
        @param topic: The topic to which the message will be published.

        @type msg: str
        @param msg: The message to be published
        """
        try:
            # send(topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None)
            self.producer.send(topic, msg)
            self.producer.flush()
        except KafkaTimeoutError:
            LOGGER.warning("Kafka timed out")
