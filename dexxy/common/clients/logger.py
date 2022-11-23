import logging
import sys

class LoggingStuff(object):
    """
    The purpsose of this class is to log messagges so we can monitor the progress during runtime. 
    """
    @property
    def logger(self, level: str = 'INFO', **kwargs):
        logging.basicConfig(stream=sys.stdout, level=level, format='%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s', **kwargs)
        name = '.'.join([self.__class__.__module__, self.__class__.__name__])
        return logging.getLogger(name)
