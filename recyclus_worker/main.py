from pathlib import Path
from redis import Redis
import logging
import json
from subprocess import CalledProcessError

from .job import Job

cache = Redis(host='redis', db=0, decode_responses=True) #  socket_connect_timeout=2, socket_timeout=2)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def next_job():
    key = cache.brpoplpush('q:submit', 'q:running')
    params = {
        'sim': cache.hgetall(f'{key}:sim')
    }
    job = Job(key, cache.hmget(key, 'jobid'), params)

    return job


def save_job(job):
    logger.debug('saving %s', job.jobid)

    cache.hset(job.key, 'status', 'storing')
    # r = requests.post('')
    # send to data services


def post_processing(job):
    logger.debug('post %s', job.jobid)
    cache.hset(job.key, 'status', 'post_processing')
    # send to data services


def report_error(job, e):
    logger.error('job: %s: error %s', job.jobid, str(e))


import sys
import socket


def main():
    logging.info('Cyclus worker stated on %s', socket.gethostname())

    while True:
        job = next_job()
        logger.info('worker job: %s', job.key)
        try:
            cache.hset(job.key, 'status', 'running')
            job.run_sim()

            save_job(job)

            post_processing(job)

            cache.hset(job.key, 'status', 'done')
        except CalledProcessError as e:
            report_error(job, e)
            cache.hmset(job.key, {'status': 'failed', 'error': e.stderr})
        finally:
            logger.info('job done: %s', job.key)


if __name__ == '__main__':
    main()
