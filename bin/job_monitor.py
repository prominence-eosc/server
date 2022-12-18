"""Update job status when needed"""
import asyncio
import json
import signal
import sys

import nats

from prominence.database import Database
from prominence.utilities import config, set_logger

logger = set_logger(config().get('job_monitor', 'log'))

def update_job_start(job, data):
    """
    Update job details
    """
    if 'execution' not in job:
        job['execution'] = {}

    job['execution']['worker'] = data['worker']

    return job

def update_job(job, data):
    """
    Update job details for a job in a terminal state
    """
    if 'execution' not in job:
        job['execution'] = {}

    if 'details' in data:
        if 'site' in data['details']:
            job['execution']['site'] = data['details']['site']
        if 'cpu_vendor' in data['details'] and 'cpu_model' in data['details'] and 'cpu_clock' in data['details']:
            job['execution']['cpu'] = {}
            job['execution']['cpu']['clock'] = data['details']['cpu_clock']
            job['execution']['cpu']['model'] = data['details']['cpu_model']
            job['execution']['cpu']['vendor'] = data['details']['cpu_vendor']
        tasks = []
        if 'tasks' in data['details']:
            for task in data['details']['tasks']:
                if 'exitCode' in task:
                    tasks.append(task)
        if tasks:
            job['execution']['tasks'] = tasks

    return job

async def run():
    async def error_cb(e):
        logger.error(e)

    async def disconnected_cb():
        logger.error('Got disconnected from NATS')

    async def reconnected_cb():
        logger.error('Got reconnected to NATS')

    async def closed_cb():
        logger.error('Stopped reconnection to NATS')

    async def subscribe_handler(msg):
        data = msg.data.decode()
        data = json.loads(data)
        logger.info('Updating job %s', data['id'])
        db = Database()
        job = db.get_job(data['id'], False)

        if data['event'] == 'start':
            logger.info('Job %s status set to running', data['id'])
            job['events']['startTime'] = data['epoch']
            job['status'] = 'running'
            job = update_job_start(job, data)
            job.save()
        elif data['event'] == 'success':
            logger.info('Job %s status set to completed', data['id'])
            job['events']['endTime'] = data['epoch']
            job['status'] = 'completed'
            job = update_job(job, data)
            job.save()
        elif data['event'] in ('failed', 'killed', 'deleted'):
            logger.info('Job %s status set to %s', data['id'], data['event'])
            job['events']['endTime'] = data['epoch']
            job['status'] = data['event']
            job = update_job(job, data)
            job.save()

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

    def signal_handler():
        if nc.is_closed:
            return
        asyncio.create_task(nc.drain())

    for sig in ('SIGINT', 'SIGTERM'):
        asyncio.get_running_loop().add_signal_handler(getattr(signal, sig), signal_handler)

    await nc.subscribe("job.status.*", cb=subscribe_handler)


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    try:
        loop.run_forever()
    finally:
        loop.close()

if __name__ == '__main__':
    main()
