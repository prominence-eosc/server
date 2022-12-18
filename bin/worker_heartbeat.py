"""Find and delete any lost workers"""
import asyncio
import json
import sys
import time

import nats

from prominence.utilities import config, set_logger

logger = set_logger(config().get('worker_heartbeat', 'log'))

async def checks():
    """
    """
    async def error_cb(err):
        logger.error(err)

    async def disconnected_cb():
        logger.error('Got disconnected from NATS')

    async def reconnected_cb():
        logger.error('Got reconnected to NATS')

    async def closed_cb():
        logger.error('Stopped reconnection to NATS')

    logger.info('Finding lost workers...')

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

    js = nc.jetstream()
    kv = await js.key_value(bucket=config().get('nats', 'workers_bucket'))
    worker_ids = await kv.keys()
    invalids = []
    for worker in worker_ids:
        entry = await kv.get(worker)
        worker = json.loads(entry.value)
        if time.time() - worker['epoch'] > int(config().get('worker_heartbeat', 'lost_after')):
            invalids.append(worker['name'])
            print(worker['name'])

    for worker in invalids:
        logger.info('Deleting worker %s', worker)
        await kv.delete(worker)

    logger.info('Finished')

    await nc.close()

def main():
    while True:
        asyncio.run(checks())
        time.sleep(int(config().get('worker_heartbeat', 'interval')))

if __name__ == '__main__':
    main()
