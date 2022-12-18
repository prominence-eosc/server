import asyncio
import signal
import sys

import nats

from prominence.utilities import config, set_logger

logger = set_logger(config().get('job_logger', 'log'))

async def run():
    async def error_cb(e):
        logger.error(e)

    async def reconnected_cb():
        logger.error('Got reconnected to NATS')

    async def closed_cb():
        logger.error('Stopped reconnection to NATS')

    async def subscribe_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        with open(f"{config().get('job_logger', 'directory')}/{subject}", 'a') as fh:
            fh.write(data)

    nc = None
    try:
        nc = await nats.connect(config().get('nats', 'url'),
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

    await nc.subscribe(f"job.{sys.argv[1]}.*", cb=subscribe_handler)


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == '__main__':
    main()
