import configparser
import os
import logging
from logging.handlers import RotatingFileHandler

def config():
    """
    Get configuration
    """
    if 'PROMINENCE_WORKER_CONFIG' in os.environ:
        filename = os.environ['PROMINENCE_WORKER_CONFIG']
    else:
        filename = '/etc/prominence/prominence.ini'

    try:
        config = configparser.ConfigParser()
        config.read(filename)
    except:
        return None

    return config

def set_logger(filename):
    """
    Setup logger
    """
    handler = RotatingFileHandler(filename, maxBytes=10485760, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
