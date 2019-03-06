import os
import logging
from subprocess import Popen, CalledProcessError
from threading import Thread
from pathlib import Path
from redis import Redis
import requests
from requests_toolbelt import MultipartEncoder

logger = logging.getLogger(__name__)
datastore = 'http://datastore:5020/api/internal/store'

cache = Redis(host='redis', db=0, decode_responses=True) #  socket_connect_timeout=2, socket_timeout=2)


class Job(Thread):
    def __init__(self, name, wdir):
        super().__init__()
        self.daemon = True
        self.name = name
        self.wdir = Path(wdir)
        self.wdir.mkdir(parents=True, exist_ok=True)
        self.files = {}
        self.key = None
        self.jobid = None

        self.start()

    def run(self):
        while True:
            self.cleanup()
            self.key = cache.brpoplpush('q:submit', 'q:running')
            cache.hset(self.key, 'ctrl', 'run')
            cache.hset(self.key, 'status', 'running')
            self.jobid = cache.hget(self.key, 'jobid')

            try:
                code = self.run_sim()
                self.save_files()

                if code == 0:
                    cache.hset(self.key, 'status', 'done')
                elif code < 0:
                    cache.hmset(self.key, {'status': 'canceled', 'error': str(code)})
                else:
                    cache.hmset(self.key, {'status': 'failed', 'error': str(code)})

            except CalledProcessError as e:
                logger.error('job %s: exception %s', self.jobid, str(e))
                cache.hmset(self.key, {'status': 'failed', 'error': e.stderr})

            except Exception as e:
                logger.error('job %s: exception %s', self.jobid, str(e))
                cache.hmset(self.key, {'status': 'failed', 'error': str(e)})

    def cleanup(self):
        self.key = None
        self.jobid = None
        self.files = {}

    def run_sim(self):
        sim = cache.hgetall(f'{self.key}:sim')
        self.set_files(sim)
        cmd = f'sleep 30; cyclus -o {self.files["output"]} {self.files["scenario"]}'

        with open(self.wdir / 'output.txt', 'wb') as out:
            with open(self.wdir / 'stderr.txt', 'wb') as err:
                cache.hset(self.key, 'status', 'running:sim')
                process = Popen(cmd, shell=True, stdout=out, stderr=err)
                logger.debug("Thread %s running process %d", self.jobid, process.pid)
                while True:
                    if process.poll() is not None:
                        break
                    if cache.hget(self.key, 'ctrl') == 'cancel':
                        process.terminate()
                        process.wait()
                        break
        return process.returncode

    def set_files(self, sim):
        for f in self.wdir.iterdir():
            os.remove(str(f))

        scenario_file = str(self.wdir / sim.get('scenario_filename', 'scenario.json'))
        output_file = str(self.wdir / f'cyclus.{sim["format"]}')

        self.files['scenario'] = scenario_file
        self.files['output'] = output_file
        self.files['stdio'] = str(self.wdir / 'output.txt')
        self.files['stderr'] = str(self.wdir / 'stderr.txt')

        with open(scenario_file, 'w') as f:
            f.write(str(sim['scenario']))

    def save_files(self):
        # cache.hset(self.key, 'status', 'running:storing sim')
        data = {
            'user': cache.hget(self.key, 'user'),
            'name': cache.hget(self.key, 'name'),
            'jobid': self.jobid
        }

        for name, filename in self.files.items():
            if Path(filename).exists():
                data[name] = (Path(filename).name, open(filename, 'rb'), 'text/plain')
        m = MultipartEncoder(data)

        r = requests.post(datastore, data=m,
                          headers={'Content-Type': m.content_type})
        logger.debug('job %d: file sent. status code %s text: %s', self.jobid, r.status_code, r.text)
