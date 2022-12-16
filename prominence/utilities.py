import configparser
import os

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
