import os
import logging
import subprocess
from pathlib import Path


logger = logging.getLogger(__name__)

jobs_path = Path('/jobs/')


def clean_directory():
    for f in jobs_path.iterdir():
        os.remove(str(f))


class Job(object):
    def __init__(self, key, jobid, params):
        logger.debug('new job: %s', jobid)
        self.key = key
        self.jobid = jobid
        self.path = jobs_path
        self.files = {}
        self.set_files(params['sim'])

    def set_files(self, sim):
        clean_directory()

        filename = sim.get('scenario_filename', 'scenario.json')
        scenario_file = str(self.path / filename)
        output_file = str(self.path / f'cyclus.{sim["format"]}')

        self.files = dict(
            scenario=scenario_file,
            output=output_file
        )
        self.path.mkdir(parents=True, exist_ok=True)
        with open(scenario_file, 'w') as f:
            f.write(str(sim['scenario']))

    def run_sim(self):
        cmd = f'cyclus -o {self.files["output"]} {self.files["scenario"]}'
        status = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(jobs_path / 'output.txt', 'wb') as f:
            f.write(status.stdout)
        with open(jobs_path / 'stderr.txt', 'wb') as f:
            f.write(status.stderr)
        status.check_returncode()

