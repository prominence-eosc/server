"""Add and update workers as necessary"""
import asyncio
import json
import signal
import sys

import logging
from logging.handlers import RotatingFileHandler

import nats

from prominence.utilities import config, set_logger

logger = set_logger(config().get('worker_handler', 'log'))

async def run():
    async def error_cb(err):
        logger.error(err)

    async def disconnected_cb():
        logger.error('Got disconnected from NATS')

    async def reconnected_cb():
        logger.error('Got reconnected to NATS')

    async def closed_cb():
        logger.error('Stopped reconnection to NATS')

    nc = None
    try:
        nc = await nats.connect(config().get('nats', 'url'),
                                disconnected_cb=disconnected_cb,
                                reconnected_cb=reconnected_cb,
                                closed_cb=closed_cb,
                                error_cb=error_cb)
    except Exception as err:
        logger.error('Got exception connecting to NATS: %s', str(err))
        sys.exit(1)

    async def subscribe_handler(msg):
        data = msg.data.decode()
        data = json.loads(data)
        logger.info('Updating worker %s', data['name'])

        js = nc.jetstream()
        kv = await js.key_value(bucket=config().get('nats', 'workers_bucket'))
        await kv.put(data['name'], (json.dumps(data)).encode('utf-8'))

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
