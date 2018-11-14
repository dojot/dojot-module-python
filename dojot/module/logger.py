"""
Logging module
"""

import logging
from logging import config as config_log
from colorlog import ColoredFormatter

class Log:
    """
    Class for logging
    """

    def __init__(self, LOG_LEVEL=logging.DEBUG,
                 LOG_FORMAT="[%(log_color)s%(asctime)-8s%(reset)s] |%(log_color)s%(module)-8s%(reset)s| %(log_color)s%(levelname)s%(reset)s: %(log_color)s%(message)s%(reset)s", DISABLED=False):

        #Disable all others modules logs
        log_config = {
            'version': 1,
            'disable_existing_loggers': True,
        }

        date_format = '%d/%m/%y - %H:%M:%S'
        config_log.dictConfig(log_config)
        self.formatter = ColoredFormatter(LOG_FORMAT, date_format)
        self.log = logging.getLogger('dojomodulepython.' + __name__)
        self.log.setLevel(LOG_LEVEL)
        self.log.disabled = DISABLED

        if not getattr(self.log, 'handler_set', None):
            self.stream = logging.StreamHandler()
            self.stream.setLevel(LOG_LEVEL)
            self.stream.setFormatter(self.formatter)
            self.log.setLevel(LOG_LEVEL)
            self.log.addHandler(self.stream)
            self.log.handler_set = True

    def color_log(self):
        """
        Returns a logger
        """
        return self.log
