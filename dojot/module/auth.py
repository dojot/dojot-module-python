"""
Authentication module
"""

import base64
import json
import requests
from .logger import Log

LOGGER = Log().color_log()
class Auth:
    """
    Class responsible for authentication mechanisms in dojot
    """
    def __init__(self, config):
        self.config = config

    def get_management_token(self, tenant):
        """
        Retrieves a token for management operations
        @type tenant: str
        @param tenant: the management tenant
        """

        userinfo = {
            "username": self.config.dojot["management_service"],
            "service": tenant
        }

        jwt = "{}.{}.{}".format(base64.b64encode("model".encode()).decode(),
                                base64.b64encode(json.dumps(
                                    userinfo).encode()).decode(),
                                base64.b64encode("signature".encode()).decode())

        return jwt

    def get_tenants(self):
        """
        Retrieves all tenants
        @return list of tenants
        """
        url = self.config.auth['url'] + "/admin/tenants"
        ret = requests.get(url, headers={'authorization': self.get_management_token(
            self.config.dojot['management_service'])})
        payload = ret.json()

        return payload['tenants']
