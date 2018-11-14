from kafka import KafkaConsumer
from kafka.errors import KafkaTimeoutError
import json
import time
import threading
from ..Logger import Log
from .TopicManager import TopicManager

LOGGER = Log().color_log()

class Consumer(threading.Thread):

    def __init__(self, group_id, config, name=None):
        threading.Thread.__init__(self)
        self.topics = []
        self.broker = config.kafka['consumer']['metadata.broker.list'].split(',')
        self.group_id = group_id
        self.consumer = None
        LOGGER.info("Creating consumer on %s on group %s and topic %s" % (self.broker, self.group_id, self.topics))
        self.consumer = KafkaConsumer(bootstrap_servers=self.broker, group_id=self.group_id)
        LOGGER.info("Consumer created %s" % self.topics)

    def subscribe(self, topic, callback):
        try:
            LOGGER.info("Got a new topic to subscribe: %s" % topic)
            LOGGER.info("Current topic list: %s" % self.topics)
            self.topics.append(topic)
            self.consumer.subscribe(topics=self.topics)
            LOGGER.debug("Current subscriptions: %s" % (self.consumer.subscription()))
            LOGGER.info("Subscribed to topic %s" % topic)
            LOGGER.debug("Current topic list: %s" % self.topics)
            self.callback = callback
        except Exception as error:
            LOGGER.warning("Something went wrong while subscribing to topic %s: %s" % (topic, error))

    def run(self):
        LOGGER.debug("Now running consumer for topics: %s" % self.topics)
        for msg in self.consumer:
            try:
                self.callback(msg.topic, msg.value)
            except Exception as error:
                LOGGER.info("Data handler raised an unknown exception. Ignoring: %s" % error)
