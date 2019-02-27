import logging
import socket
from pathlib import Path
from subprocess import CalledProcessError

from redis import Redis
import requests
from requests_toolbelt import MultipartEncoder

from .job import Job

cache = Redis(host='redis', db=0, decode_responses=True) #  socket_connect_timeout=2, socket_timeout=2)

hostname = f'worker-{socket.gethostname()}'

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def next_job():
    key = cache.brpoplpush('q:submit', 'q:running')
    params = {
        'sim': cache.hgetall(f'{key}:sim')
    }
    job = Job(key, cache.hget(key, 'jobid'), params)

    return job


datastore = 'http://datastore:5020/api/internal/store'


def save_job(job):
    logger.debug('saving %s...', job.jobid)

    cache.hset(job.key, 'status', 'storing')

    data = {
        'user': cache.hget(job.key, 'user'),
        'name': cache.hget(job.key, 'name'),
        'jobid': job.jobid,
    }

    for name, filename in job.files.items():
        data[name] = (Path(filename).name, open(filename, 'rb'), 'text/plain')
    m = MultipartEncoder(data)

    r = requests.post(datastore, data=m,
                      headers={'Content-Type': m.content_type})
    logger.debug('reply: %s %s', r.status_code, r.text)


def post_processing(job):
    cache.hset(job.key, 'status', 'post_processing')
    # send to data services


def report_error(job, e):
    logger.error('job: %s: error %s', job.jobid, str(e))
    save_job(job)


def main():
    logging.info('Cyclus worker stated on %s', hostname)

    while True:
        job = next_job()
        logger.info('worker job: %s %s', job.key, job.jobid)
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
