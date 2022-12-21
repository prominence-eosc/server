"""Match jobs to workers"""
import asyncio
import json
import sys
import time
import nats

from prominence.database import Database
from prominence.utilities import config, set_logger

logger = set_logger(config().get('matcher', 'log'))

db = Database()

async def matcher():
    """
    Match idle jobs to workers
    """
    async def error_cb(err):
        logger.error(err)

    async def disconnected_cb():
        logger.error('Got disconnected from NATS')

    async def reconnected_cb():
        logger.error('Got reconnected to NATS')

    async def closed_cb():
        logger.error('Stopped reconnection to NATS')

    logger.info('Starting matching...')
    start_time = time.time()

    logger.info('Getting idle jobs...')
    jobs = db.get_pending_jobs()
    idle_jobs = []
    for job in jobs:
        idle_jobs.append(job)
    logger.info('There are %d idle jobs', len(idle_jobs))

    logger.info('Getting workers...')
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

    async def send(id, job):
        data = json.dumps(job).encode('utf-8')
        await nc.publish("worker.job.%s" % id, data)

    js = nc.jetstream()
    kv = await js.key_value(bucket=config().get('nats', 'workers_bucket'))
    worker_ids = []
    try:
        worker_ids = await kv.keys()
    except:
        logger.info('No workers found')
    workers = []
    workers_resources = {}
    for worker in worker_ids:
        try:
            entry = await kv.get(worker)
            worker = json.loads(entry.value)
            if worker['status'] == 'ready':
                workers.append(worker)
                workers_resources[worker['name']] = worker['resources']['available']
        except:
            pass

    logger.info('Matching...')
    for job in idle_jobs:
        for worker in workers:
            if (workers_resources[worker['name']]['cpus'] >= job['resources']['cpus'] and
                workers_resources[worker['name']]['memory'] >= job['resources']['memory'] and
                workers_resources[worker['name']]['disk'] >= job['resources']['disk']):
                logger.info('Job %s matched to worker %s', job['id'], worker['name'])
                await send(worker['name'], {'create': job})
                db.update_status(job['id'], 'assigned')
                workers_resources[worker['name']]['cpus'] -= job['resources']['cpus']
                workers_resources[worker['name']]['memory'] -= job['resources']['memory']
                workers_resources[worker['name']]['disk'] -= job['resources']['disk']
                break

    logger.info('Finished, took %f secs', time.time() - start_time)
    await nc.close()

def main():
    while True:
        asyncio.run(matcher())
        time.sleep(int(config().get('matcher', 'interval')))

if __name__ == '__main__':
    main()
