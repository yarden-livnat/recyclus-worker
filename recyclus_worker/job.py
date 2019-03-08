from datetime import datetime
import json
import logging
import os
from pathlib import Path
from redis import Redis
import requests
from requests_toolbelt import MultipartEncoder
from subprocess import Popen, CalledProcessError, STDOUT
from threading import Thread
import time

PROCESS_MONITOR_DELAY = 5

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
        self.tasks = {}
        self.jobid = None
        self.logid = None

        self.start()

    def log(self, fields):
        cache.xadd(self.logid, fields)

    def run(self):
        while True:
            self.cleanup()
            self.key = cache.brpoplpush('q:submit', 'q:running')
            cache.hset(self.key, 'ctrl', 'run')
            cache.hset(self.key, 'status', 'running')
            self.jobid = cache.hget(self.key, 'jobid')
            self.tasks = json.loads(cache.hget(self.key, 'tasks'))
            # self.logid = cache.hget(self.key, 'logid')
            logger.debug('job %s started', self.jobid)

            # self.log({'status': 'running'})

            status = self.run_sim()
            if status['status'] == 'done':
                status = self.run_post()

            cache.hset(self.key, 'status',status['status'])
            cache.lrem('q:running', 1, self.key)
            cache.lpush('q:done', self.key)
            logger.debug('job %s ended', self.jobid)

    def cleanup(self):
        self.key = None
        self.jobid = None
        self.files = {}
        self.tasks = {}
        for f in self.wdir.iterdir():
            os.remove(str(f))

    def run_sim(self):
        if 'simulation' not in self.tasks:
            return
        logger.debug('job %s run_sim', self.jobid)
        cache.hset(self.key, 'status', f'running:simulation')
        task = self.tasks['simulation']

        if 'files' not in task:
            task['files'] = ['scenario', 'output', 'stdio']

        scenario_file = str(self.wdir / task.get('scenario_filename', 'scenario.json'))
        output_file = str(self.wdir / f'cyclus.{task["format"]}')

        self.files['scenario'] = scenario_file
        self.files['output'] = output_file
        self.files['stdio'] = str(self.wdir / 'simulation_output.txt')

        with open(scenario_file, 'w') as f:
            f.write(str(task['scenario']))

        cmd = f'cyclus -o {self.files["output"]} {self.files["scenario"]}'
        return self.run_task('simulation', cmd, task)

    def run_post(self):
        if 'post' not in self.tasks:
            return

        logger.debug('job %s run_post', self.jobid)
        cache.hset(self.key, 'status', f'running:post')
        task = self.tasks['post']
        if 'files' not in task:
            task['files'] = ['script', 'stdio']

        script_file = str(self.wdir / task.get('script_filename', 'post.py'))

        self.files = {
            'script': script_file
        }

        with open(script_file, 'w') as f:
            f.write(str(task['script']))

        cmd = f'python {script_file}'
        return self.run_task('post', cmd, task)

    def run_task(self, name, cmd, opts):
        status_key = f'{self.key}:status:{name}'

        logger.debug('job %s run_task', self.jobid)
        delay = opts.get('sleep', 0)
        if delay > 0:
            cmd = f'sleep {delay}; {cmd}'
        start_time = datetime.now()
        cache.hset(status_key, 'start time', str(start_time))

        status = {
            'started at': str(datetime.now()).split('.', 2)[0]
        }
        try:
            self.files['stdio'] = self.wdir / f'{name}_output.txt'
            with open(self.wdir / 'output.txt', 'wb') as out:
                process = Popen(cmd, shell=True, stdout=out, stderr=STDOUT)
                while True:
                    time.sleep(PROCESS_MONITOR_DELAY)
                    if process.poll() is not None:
                        break
                    if cache.hget(self.key, 'ctrl') == 'cancel':
                        process.terminate()
                        process.wait()
                    break
            if process.returncode == 0:
                status['status'] = 'done'
            elif process.returncode > 0:
                status['status'] = 'failed'
                status['reason'] = f'exit code:{process.returncode}'
            else:
                status['status'] = 'canceled'
        except (CalledProcessError, Exception) as e:
            logger.error('job %s: exception %s', self.jobid, str(e))
            status['status'] = 'failed'
            status['reason'] = str(e)
        finally:
            end_time = datetime.now()
            files = [name for name in self.files if name in opts['files']]
            self.save_files(files)
            status['ended at'] = str(end_time).split('.', 2)[0]
            status['duration'] = str(end_time - start_time).split('.', 2)[0]
            status['files'] = json.dumps(files)

            cache.hmset(status_key, status)
        return status

    def save_files(self, files):
        data = {
            'user': cache.hget(self.key, 'user'),
            'name': cache.hget(self.key, 'name'),
            'jobid': self.jobid
        }

        for name, filename in self.files.items():
            if name in files and Path(filename).exists():
                data[name] = (Path(filename).name, open(filename, 'rb'), 'text/plain')
        m = MultipartEncoder(data)

        r = requests.post(datastore, data=m,
                          headers={'Content-Type': m.content_type})
        logger.debug('job %s: file sent. status code %d text: %s', self.jobid, r.status_code, r.text)
