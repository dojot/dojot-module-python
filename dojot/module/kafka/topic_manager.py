"""
Topic manager
"""
import requests
from .. import Auth
from ..logger import Log

LOGGER = Log().color_log()
class TopicManager():
    """
    Class that deals with DataBroker topics
    """

    def __init__(self, config):
        """
        TopicManager object initialization

        @type config: dojot.module.Config
        @param config: The configuration object
        """
        self.topics = dict()
        self.tenants = []
        self.broker = config.data_broker["url"]
        self.auth = Auth(config)

    @staticmethod
    def get_key(tenant, subject):
        """
        Return a key to be associated with a particular Kafka topic

        @type tenant: str
        @param tenant: The tenant to be used

        @type subject: str
        @param subject: The subject to be used
        """
        return tenant + ":" + subject

    def get_topic(self, tenant, subject, global_val=""):
        """
        Retrieves a topic from DataBroker

        @type tenant: str
        @param tenant: The tenant associated to the topic being retrieved

        @type subject: str
        @param subject: The subject associated to the topic being retrieved

        @type global_val: bool
        @param global_val: True if this topic should be a global one.
        """
        key = self.get_key(tenant, subject)

        if key in self.topics:
            return self.topics[key]

        if global_val != "":
            querystring = "?global=true"
        else:
            querystring = ""

        url = self.broker + "/topic/" + subject + querystring
        ret = requests.get(url, headers={
            'authorization': "Bearer " + self.auth.get_management_token(tenant)})
        try:
            payload = ret.json()
        except ValueError as error:
            LOGGER.error("Returned topic is not a JSON object: %s", error)
            LOGGER.error("Returned data is: %s", ret.msg)
            return None

        self.topics[key] = payload['topic']

        return self.topics[key]
