import configparser
import socket
import psutil
import asyncio
import json
import os
import time
import sqlite3
import uuid
import sys
import re, uuid
import nats
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError

import logging
from logging.handlers import RotatingFileHandler

from prominence.database import Database
from prominence.utilities import config

handler = RotatingFileHandler(config().get('matcher', 'log'), maxBytes=10485760, backupCount=10)
formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

db = Database()

async def send(id, job):
    nc = await nats.connect(config().get('server', 'nats'))
    data = json.dumps(job).encode('utf-8')
    await nc.publish("worker.job.%s" % id, data)

def matcher():
    """
    """
    logger.info('Starting matching...')

    logger.info('Getting idle jobs...')
    jobs = db.get_pending_jobs()
    idle_jobs = []
    for job in jobs:
        idle_jobs.append(job)
    logger.info('There are %d idle jobs', len(idle_jobs))

    logger.info('Getting workers...')
    workers = db.get_workers()
    idle_workers = []
    workers_resources = {}
    for worker in workers:
        idle_workers.append(worker)
        workers_resources[worker['name']] = worker['resources']['available']

    logger.info('Matching...')
    for job in idle_jobs:
        for worker in idle_workers:
            if (workers_resources[worker['name']]['cpus'] >= job['resources']['cpus'] and
                workers_resources[worker['name']]['memory'] >= job['resources']['memory'] and
                workers_resources[worker['name']]['disk'] >= job['resources']['disk']):
                   logger.info('Job %s matched to worker %s', job['id'], worker['name'])
                   asyncio.run(send(worker['name'], {'create': job}))
                   workers_resources[worker['name']]['cpus'] -= job['resources']['cpus']
                   workers_resources[worker['name']]['memory'] -= job['resources']['memory']
                   workers_resources[worker['name']]['disk'] -= job['resources']['disk']
                   break

    logger.info('Finished')

def main():
    while True:
        matcher()
        time.sleep(int(config().get('matcher', 'interval')))

if __name__ == '__main__':
    main()
