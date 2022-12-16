"""Find and delete any lost workers"""
import time

import logging
from logging.handlers import RotatingFileHandler

from prominence.database import Database
from prominence.utilities import config, set_logger

logger = set_logger(config().get('worker_heartbeat', 'log'))

def checks():
    """
    """
    logger.info('Finding lost workers...')
    db = Database()

    invalids = []
    workers = db.get_lost_workers(int(config().get('worker_heartbeat', 'lost_after')))
    for worker in workers:
        invalids.append(worker)

    for worker in invalids:
        logger.info('Deleting worker %s', worker.name)
        db.delete_worker(worker)

    logger.info('Finished')

def main():
    while True:
        checks()
        time.sleep(int(config().get('worker_heartbeat', 'interval')))

if __name__ == '__main__':
    main()
