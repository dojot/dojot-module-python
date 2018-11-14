import requests
from .. import Auth
from ..Logger import Log

LOGGER = Log().color_log()
class TopicManager():

    def __init__(self, config):
        self.topics = dict()
        self.tenants = []
        self.broker = config.data_broker["url"]
        self.auth = Auth(config)

    def get_key(self, tenant, subject):
        return tenant + ":" + subject

    def get_topic(self, tenant, subject, globalVal=""):
        
        key = self.get_key(tenant,subject)

        if(key in self.topics):
            return self.topics[key]
        else:
            if globalVal != "":
                querystring = "?global=true"
            else:
                querystring = ""

        url = self.broker + "/topic/" + subject + querystring
        ret = requests.get(url, headers={'authorization': "Bearer " + self.auth.get_management_token(tenant)})
        payload = ret.json()
        self.topics[key] = payload['topic']

        return self.topics[key]
