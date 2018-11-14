"""
Kafka consumer
"""

import threading
from kafka import KafkaConsumer
from kafka.errors import IllegalStateError
from ..logger import Log

LOGGER = Log().color_log()

class Consumer(threading.Thread):
    """
    Kafka consumer
    """
    def __init__(self, group_id, config, name=None):
        """
        Consumer object initialization

        @type group_id: str
        @param group_id: The consumer group to which this consumer will belong

        @type config: dojot.module.Config
        @param config: The configuration object

        @type name: str
        @param name: A name for this consumer.
        """
        threading.Thread.__init__(self)
        self.topics = []
        self.broker = config.kafka['consumer']['metadata.broker.list'].split(',')
        self.group_id = group_id
        self.consumer = None
        self.callback = None
        LOGGER.info("Creating consumer on %s on group %s and topic %s",
                    self.broker, self.group_id, self.topics)
        self.consumer = KafkaConsumer(bootstrap_servers=self.broker, group_id=self.group_id)
        LOGGER.info("Consumer created %s", self.topics)

    def subscribe(self, topic, callback):
        """
        Subscribe to a list of topics

        This function will add a new topic to the topics which this consumer
        is subscribed to.

        @type topic: str
        @param topic: new topic

        @type callback: function
        @param callback: Function to be called whenever a new message is received.
        This method should not thrown any exceptions. No processing is done
        with its results.
        """

        try:
            LOGGER.info("Got a new topic to subscribe: %s", topic)
            LOGGER.info("Current topic list: %s", self.topics)
            self.topics.append(topic)
            self.consumer.subscribe(topics=self.topics)
            LOGGER.debug("Current subscriptions: %s", self.consumer.subscription())
            LOGGER.info("Subscribed to topic %s", topic)
            LOGGER.debug("Current topic list: %s", self.topics)
            self.callback = callback
        except IllegalStateError as error:
            LOGGER.critical("Kafka is in illegal state: %s", error)
        except AssertionError as error:
            LOGGER.critical("No topic was provided. Should be [%s]: %s", topic, error)
        except TypeError as error:
            LOGGER.critical("Kafka listener type is invalid: %s", error)

    def run(self):
        LOGGER.debug("Now running consumer for topics: %s", self.topics)
        for msg in self.consumer:
            self.callback(msg.topic, msg.value)
