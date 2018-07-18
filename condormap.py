from pathlib import Path
import time

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
        transfer_output_remaps = ';'.join([
            f'$(ProdId).in={outputs_dir}/$(ProcId).in',
        ]),
    )
    print(submit_dict)
    sub = htcondor.Submit(submit_dict)

    schedd = htcondor.Schedd()
    with schedd.transaction() as txn:
        cluster = sub.queue(txn, len(procid_to_arg))

    return Job(cluster, job_dir, procid_to_arg, outputs_dir)


class Job:
    def __init__(self, cluster, job_dir, jobid_to_arg, outputs_dir):
        self.cluster = cluster
        self.jobdir = job_dir
        self.jobid_to_arg = jobid_to_arg
        self.outputs_dir = outputs_dir

    def __iter__(self):
        for procid in self.jobid_to_arg:
            path = self.outputs_dir / f'{procid}.out'
            if not path.exists():
                time.sleep(1)
            with path.open(mode = 'rb') as file:
                yield cloudpickle.load(file)
