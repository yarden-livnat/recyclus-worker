import logging
import socket
from pathlib import Path
from .job import Job

num_threads = 3
hostname = f'worker-{socket.gethostname()}'

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


jobs = []

wdir = Path('/jobs')


def run():
    logging.info('Cyclus worker stated on %s', hostname)

    for i in range(num_threads):
        name = f'task-{i}'
        jobs.append(Job(name, wdir / name))
        logger.info('started job %s', name)

    for job in jobs:
        job.join()

    logger.info('worker done')
