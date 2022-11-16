import logging


def LoggingMsg():
    
    # Set the message to output and level
    logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')

    # Create a logger
    loggerMsg = logging.getLogger(__name__)
    return loggerMsg