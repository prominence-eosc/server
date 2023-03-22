import asyncio
import signal
import sys

import nats

from prominence.utilities import config, set_logger

logger = set_logger(config().get('job_logger', 'log').replace('type', sys.argv[1]))

def write_log(msg):
    subject = msg.subject
    data = msg.data.decode()
    with open(f"{config().get('job_logger', 'directory')}/{subject}", 'a') as fh:
        fh.write(data)

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
    except Exception as e:
        logger.error('Got exception connecting to NATS: %s', str(e))
        sys.exit(1)

    def signal_handler():
        if nc.is_closed:
            return
        asyncio.create_task(nc.drain())

    for sig in ('SIGINT', 'SIGTERM'):
        asyncio.get_running_loop().add_signal_handler(getattr(signal, sig), signal_handler)

    js = nc.jetstream()
    sub = await js.subscribe(f"jobs.*.{sys.argv[1]}", durable=f"job-{sys.argv[1]}-handler")

    while True:
        try:
            msg = await sub.next_msg()
            await msg.ack()
            write_log(msg)
        except Exception as err:
            if 'timeout' not in str(err):
                logger.error(str(err))
            pass

    await nc.close()


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == '__main__':
    main()
