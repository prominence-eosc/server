"""Add and update workers as necessary"""
import asyncio
import json
import signal
import sys

import logging
from logging.handlers import RotatingFileHandler

import nats

from prominence.database import Database
from prominence.utilities import config, set_logger

logger = set_logger(config().get('worker_monitor', 'log'))

async def run():
    async def subscribe_handler(msg):
        data = msg.data.decode()
        data = json.loads(data)
        logger.info('Updating worker %s', data['name'])
        db = Database()
        db.add_or_update_worker(data)

    nc = None
    try:
        nc = await nats.connect(config().get('nats', 'url'))
    except Exception as e:
        logger.error('Got exception connecting to NATS: %s', str(e))
        sys.exit(1)

    def signal_handler():
        if nc.is_closed:
            return
        asyncio.create_task(nc.drain())

    for sig in ('SIGINT', 'SIGTERM'):
        asyncio.get_running_loop().add_signal_handler(getattr(signal, sig), signal_handler)

    await nc.subscribe("worker.status.*", cb=subscribe_handler)

def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    try:
        loop.run_forever()
    finally:
        loop.close()

if __name__ == '__main__':
    main()
