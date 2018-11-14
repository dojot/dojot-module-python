from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
import json
from .TopicManager import TopicManager
from ..Logger import Log

LOGGER = Log().color_log()

class Producer:

    def __init__(self, config):
        self.broker = config.kafka['producer']['metadata.broker.list'].split(',')

    def init(self):
        try:
            self.producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                          bootstrap_servers=self.broker)
            return 1
        except AssertionError as error:
            LOGGER.warning("Ignoring assertion error on kafka producer %s" % error)
            return 0

    def produce(self, topic, msg):
        try:
            # send(topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None)
            self.producer.send(topic, msg)
            self.producer.flush()
        except KafkaTimeoutError:
            LOGGER.warning("Kafka timed out")
