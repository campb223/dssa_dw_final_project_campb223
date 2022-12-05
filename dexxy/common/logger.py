import logging
import sys

class LoggingStuff(object):
    """
    The purpsose of this class is to log messagges so we can monitor the progress during runtime. 
    
    Example of runtime output:
        2022-12-02 19:03:00,764 :: Worker :: INFO :: Running Tasks tearDown on Worker 1 
    """
    @property
    def logger(self, level: str = 'INFO', **kwargs):
        logging.basicConfig(stream=sys.stdout, level=level, format='%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s', **kwargs)
        name = self.__class__.__name__ 
        
        return logging.getLogger(name)