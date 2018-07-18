from pathlib import Path
import time
import collections
import enum

import htcondor
import cloudpickle


def condormap(fn, args):
    job_dir = Path.cwd() / 'tmp'
    inputs_dir = job_dir / 'inputs'
    outputs_dir = job_dir / 'outputs'
    logs_dir = job_dir / 'logs'
    for path in (job_dir, inputs_dir, outputs_dir, logs_dir):
        path.mkdir(parents = True, exist_ok = True)

    fn_path = (job_dir / 'fn.pkl')
    with fn_path.open(mode = 'wb') as file:
        cloudpickle.dump(fn, file)

    procid_to_arg = {index: arg for index, arg in enumerate(args)}

    for index, arg in procid_to_arg.items():
        with (inputs_dir / f'{index}.in').open(mode = 'wb') as file:
            cloudpickle.dump(arg, file)

    submit_dict = dict(
        executable = str(Path(__file__).parent / 'run.sh'),
        arguments = '$(ProcId)',
        log = str(job_dir / 'job.log'),
        output = str(logs_dir / '$(ProcId).output'),
        error = str(logs_dir / '$(ProcId).error'),
        should_transfer_files = 'YES',
        when_to_transfer_output = 'ON_EXIT',
        request_cpus = '1',
        request_memory = '100MB',
        request_disk = '5GB',
        transfer_input_files = ','.join([
            'http://proxy.chtc.wisc.edu/SQUID/karpel/condormap.tar.gz',
            str(Path(__file__).parent / 'run.py'),
            str(inputs_dir / '$(ProcId).in'),
            str(fn_path),
        ]),
        transfer_output_remaps = '"' + ';'.join([
            f'$(ProcId).out={outputs_dir / "$(ProcId).out"}',
        ]) + '"',
    )
    print(submit_dict)
    sub = htcondor.Submit(submit_dict)

    schedd = htcondor.Schedd()
    with schedd.transaction() as txn:
        clusterid = sub.queue(txn, len(procid_to_arg))

    return Job(clusterid, job_dir, procid_to_arg, outputs_dir)


class JobStatus(enum.IntEnum):
    IDLE = 1
    RUNNING = 2
    REMOVED = 3
    COMPLETED = 4
    HELD = 5
    TRANSFERRING_OUTPUT = 6
    SUSPENDED = 7

    def __str__(self):
        return JOB_STATUS_STRINGS[self]


JOB_STATUS_STRINGS = {
    JobStatus.IDLE: 'Idle',
    JobStatus.RUNNING: 'Running',
    JobStatus.REMOVED: 'Removed',
    JobStatus.COMPLETED: 'Completed',
    JobStatus.HELD: 'Held',
    JobStatus.TRANSFERRING_OUTPUT: 'Transferring Output',
    JobStatus.SUSPENDED: 'Suspended',
}


class Job:
    def __init__(self, clusterid, job_dir, jobid_to_arg, outputs_dir):
        self.clusterid = clusterid
        self.job_dir = job_dir
        self.procid_to_arg = jobid_to_arg
        self.outputs_dir = outputs_dir

    def __iter__(self):
        for procid in self.procid_to_arg:
            path = self.outputs_dir / f'{procid}.out'
            while not path.exists():
                time.sleep(1)
            with path.open(mode = 'rb') as file:
                yield cloudpickle.load(file)

    def iter_as_available(self):
        paths = [self.outputs_dir / f'{procid}.out' for procid in self.procid_to_arg]
        while len(paths) > 0:
            for path in paths:
                if not path.exists():
                    continue
                with path.open(mode = 'rb') as file:
                    paths.remove(path)
                    yield cloudpickle.load(file)
            time.sleep(1)

    def query(self, projection = []):
        yield from htcondor.Schedd().xquery(
            requirements = f'ClusterId=={self.clusterid}',
            projection = projection,
        )

    def status(self):
        query = tuple(self.query(projection = ['JobStatus', 'ProcId']))

        status_counts = collections.Counter(JobStatus(classad['JobStatus']) for classad in query)

        procids = sorted(classad['ProcId'] for classad in query)
        procid_ranges = []
        start = procids[0]
        previous = procids[0]
        for procid in procids[1:]:
            if procid != previous + 1:
                print('triggered', start, previous)
                procid_ranges.append((start, previous))
                start = procid
            if procid == procids[-1]:
                procid_ranges.append((start, procid))
            previous = procid

        status_str = 'JobStatus Counts: ' + ', '.join(f'{str(status)}: {count}' for status, count in status_counts.items())
        procid_str = f'ClusterId: {self.clusterid}, ' + 'ProcIds: ' + ', '.join(f'{start}...{stop}' for start, stop in procid_ranges)

        return '\n'.join((status_str, procid_str))
