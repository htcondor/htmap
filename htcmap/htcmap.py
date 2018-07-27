from typing import Any, Tuple, Iterable, Dict, Union, Optional, List, Callable

from pathlib import Path
import time
import hashlib
import itertools
from copy import deepcopy, copy

import htcondor
from htcondor import JobAction
import cloudpickle

from .settings import settings


def hash_bytes(bytes: bytes) -> str:
    return hashlib.md5(bytes).hexdigest()


def save(obj: Any, path: Path):
    with path.open(mode = 'wb') as file:
        cloudpickle.dump(obj, file)


def to_bytes(obj: Any) -> bytes:
    return cloudpickle.dumps(obj)


def save_bytes(bytes, path: Path):
    with path.open(mode = 'wb') as file:
        file.write(bytes)


def load(path: Path) -> Any:
    with path.open(mode = 'rb') as file:
        return cloudpickle.load(file)


def map(func, args, **kwargs) -> 'MapResult':
    return htcmap(func).map(args, **kwargs)


def productmap(func, *args, **kwargs) -> 'MapResult':
    return htcmap(func).productmap(*args, **kwargs)


def starmap(func, args, kwargs) -> 'MapResult':
    return htcmap(func).starmap(args, kwargs)


def build_job(func):
    return htcmap(func).build_job()


def htcmap(name: Optional[str] = None, submit_descriptors: Optional[Dict] = None):
    def wrapper(func):
        return HTCMapper(
            func,
            name = name if isinstance(name, str) else func.__name__,
            submit_descriptors = submit_descriptors,
        )

    # if called like @htcmap, without parens, name is actually the function
    if callable(name):
        return wrapper(name)

    return wrapper


class MapResult:
    # todo: specialized versions of query to do condor_q, condor_q --held
    def __init__(self, mapper: 'HTCMapper', clusterid: Optional[int], hashes: Iterable[str]):
        self.mapper = mapper
        self.clusterid = clusterid
        self.hashes = tuple(hashes)

        if self.clusterid is None:
            print('No new hashes, no jobs were submitted')

    @classmethod
    def from_clusterid(cls, mapper: 'HTCMapper', clusterid: Union[int, str]):
        with (mapper.hashes_dir / f'{clusterid}.hashes').open() as file:
            hashes = (h.strip() for h in file)

            return cls(mapper = mapper, clusterid = clusterid, hashes = hashes)

    def item_to_hash(self, item: Union[int, str]) -> str:
        if isinstance(item, int):
            item = self.hashes[item]

        return item

    def __getitem__(self, item: Union[int, str]) -> Any:
        h = self.item_to_hash(item)

        path = self.mapper.outputs_dir / f'{h}.out'
        while not path.exists():
            time.sleep(1)

        return load(path)

    def __iter__(self) -> Iterable[Any]:
        for h in self.hashes:
            path = self.mapper.outputs_dir / f'{h}.out'
            while not path.exists():
                time.sleep(1)
            yield load(path)

    # todo: look at ipyparallel asyn get timeout TimeoutError

    def iter(self, callback: Callable = None):
        if callback is None:
            callback = lambda _: _

        for obj in self:
            callback(obj)
            yield obj

    def iter_with_inputs(self, callback: Callable = None) -> Iterable[Tuple[Any, Any]]:
        if callback is None:
            callback = lambda *_: _

        for h in self.hashes:
            input_path = self.mapper.inputs_dir / f'{h}.in'
            output_path = self.mapper.outputs_dir / f'{h}.out'
            while not output_path.exists():
                time.sleep(1)
            inp = load(input_path)
            out = load(output_path)
            callback(inp, out)
            yield inp, out

    def iter_as_available(self, callback: Callable = None) -> Iterable[Any]:
        if callback is None:
            callback = lambda _: _

        paths = {self.mapper.outputs_dir / f'{h}.out' for h in self.hashes}
        while len(paths) > 0:
            for path in copy(paths):
                if not path.exists():
                    continue
                with path.open(mode = 'rb') as file:
                    paths.remove(path)
                    obj = cloudpickle.load(file)
                    callback(obj)
                    yield obj
            time.sleep(1)

    def query(self, projection = None):
        if self.clusterid is None:
            yield from ()
        if projection is None:
            projection = []
        yield from htcondor.Schedd().xquery(
            requirements = f'ClusterId=={self.clusterid}',
            projection = projection,
        )

    def act(self, action: JobAction):
        return htcondor.Schedd().act(action, f'ClusterId=={self.clusterid}')

    def remove(self):
        return self.act(JobAction.Remove)

    def iter_output(self, item: Union[int, str]) -> Iterable[str]:
        h = self.item_to_hash(item)
        with (self.mapper.job_logs_dir / f'{h}.out').open() as file:
            yield from file

    def iter_error(self, item: Union[int, str]) -> Iterable[str]:
        h = self.item_to_hash(item)
        with (self.mapper.job_logs_dir / f'{h}.err').open() as file:
            yield from file

    def output(self, item: Union[int, str]):
        return ''.join(self.iter_output(item))

    def error(self, item: Union[int, str]):
        return ''.join(self.iter_error(item))

    def tail(self):
        with (self.mapper.cluster_logs_dir / f'{self.clusterid}.log').open() as file:
            file.seek(0, 2)
            while True:
                current = file.tell()
                line = file.readline()
                if line == '':
                    file.seek(current)
                    time.sleep(.1)
                else:
                    print(line, end = '')

    def __repr__(self):
        return f'{self.__class__.__name__}(mapper = {self.mapper}, clusterid = {self.clusterid})'


class JobBuilder:
    def __init__(self, mapper):
        self.mapper = mapper

        self.args = []
        self.kwargs = []

        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # todo: should do nothing if exception occurred inside with block
        self.result = self.mapper.starmap(self.args, self.kwargs)

    def __call__(self, *args, **kwargs):
        self.args.append(args)
        self.kwargs.append(kwargs)

    def __repr__(self):
        return f'{self.__class__.__name__}(mapper = {self.mapper})'


class HTCMapper:
    def __init__(self, func: Callable, name: str, submit_descriptors = None):
        self.func = func
        self.name = name
        self.submit_descriptors = submit_descriptors or {}

        self.job_dir = settings.HTCMAP_DIR / name
        self.inputs_dir = self.job_dir / 'inputs'
        self.outputs_dir = self.job_dir / 'outputs'
        self.job_logs_dir = self.job_dir / 'job_logs'
        self.cluster_logs_dir = self.job_dir / 'cluster_logs'
        self.hashes_dir = self.job_dir / 'hashes_by_clusterid'

        self._mkdirs()

        self.fn_path = self.job_dir / 'fn.pkl'
        if not self.fn_path.exists():
            save(self.func, self.fn_path)

    def _mkdirs(self):
        for path in (
            self.job_dir,
            self.inputs_dir,
            self.outputs_dir,
            self.job_logs_dir,
            self.cluster_logs_dir,
            self.hashes_dir,
        ):
            path.mkdir(parents = True, exist_ok = True)

    def __repr__(self):
        return f'{self.__class__.__name__}(name = {self.name}, func = {self.func})'

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def map(self, args, **kwargs) -> MapResult:
        args = ((arg,) for arg in args)
        args_and_kwargs = zip(args, itertools.repeat(kwargs))
        return self._map(args_and_kwargs)

    def productmap(self, *args, **kwargs) -> MapResult:
        dicts = [{}]
        for key, values in kwargs.items():
            values = tuple(values)
            dicts = [deepcopy(d) for d in dicts for _ in range(len(values))]
            for d, v in zip(dicts, itertools.cycle(values)):
                d[key] = v

        args = itertools.repeat(args)
        args_and_kwargs = zip(args, dicts)

        return self._map(args_and_kwargs)

    def starmap(self, args: Iterable[Tuple] = (), kwargs: Iterable[Dict] = ()) -> MapResult:
        args_and_kwargs = zip_args_and_kwargs(args, kwargs)
        return self._map(args_and_kwargs)

    def build_job(self):
        return JobBuilder(mapper = self)

    def _map(self, args_and_kwargs) -> MapResult:
        hashes = []
        new_hashes = []
        for a_and_k in args_and_kwargs:
            b = to_bytes(a_and_k)
            h = hash_bytes(b)
            hashes.append(h)

            # if output already exists, don't re-do it
            output_path = self.outputs_dir / f'{h}.out'
            if output_path.exists():
                continue

            input_path = self.inputs_dir / f'{h}.in'
            save_bytes(b, input_path)
            new_hashes.append(h)

        if len(new_hashes) == 0:
            return MapResult(
                mapper = self,
                clusterid = None,
                hashes = hashes,
            )

        submit_dict = {
            'JobBatchName': self.name,
            'executable': str(Path(__file__).parent / 'run' / 'run.sh'),
            'arguments': '$(Item)',
            'log': str(self.cluster_logs_dir / '$(ClusterId).log'),
            'output': str(self.job_logs_dir / '$(Item).output'),
            'error': str(self.job_logs_dir / '$(Item).error'),
            'should_transfer_files': 'YES',
            'when_to_transfer_output': 'ON_EXIT',
            'request_cpus': '1',
            'request_memory': '100MB',
            'request_disk': '5GB',
            'transfer_input_files': ','.join([
                'http://proxy.chtc.wisc.edu/SQUID/karpel/htcmap.tar.gz',
                str(Path(__file__).parent / 'run' / 'run.py'),
                str(self.inputs_dir / '$(Item).in'),
                str(self.fn_path),
            ]), 'transfer_output_remaps': '"' + ';'.join([
                f'$(Item).out={self.outputs_dir / "$(Item).out"}',
            ]) + '"'
        }
        sub = htcondor.Submit(submit_dict)

        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            submit_result = sub.queue_with_itemdata(txn, 1, iter(new_hashes))

        clusterid = submit_result.cluster()

        with (self.hashes_dir / f'{clusterid}.hashes').open(mode = 'w') as file:
            file.write('\n'.join(hashes))

        return MapResult(
            mapper = self,
            clusterid = clusterid,
            hashes = hashes,
        )

    def connect(self, clusterid: Union[int, str]):
        return MapResult.from_clusterid(self, clusterid)

    def clean(self):
        self.clean_inputs()
        self.clean_outputs()
        self.clean_job_logs()
        self.clean_cluster_logs()

    def clean_inputs(self):
        self._clean(self.inputs_dir)

    def clean_outputs(self):
        self._clean(self.outputs_dir)

    def clean_job_logs(self):
        self._clean(self.job_logs_dir)

    def clean_cluster_logs(self):
        self._clean(self.cluster_logs_dir)

    def _clean(self, target_dir):
        for path in target_dir.iter_dir():
            path.unlink()

    def status(self):
        # status of any running cluster jobs
        raise NotImplementedError

    def stats(self):
        # number of inputs and outputs, size of input/output dirs, etc.
        raise NotImplementedError


def zip_args_and_kwargs(args: Iterable[Tuple], kwargs: Iterable[Dict]):
    iterators = [iter(args), iter(kwargs)]
    fills = {0: (), 1: {}}
    num_active = 2
    while True:
        values = []
        for i, it in enumerate(iterators):
            try:
                value = next(it)
            except StopIteration:
                num_active -= 1
                if num_active == 0:
                    return
                iterators[i] = itertools.repeat(fills[i])
                value = fills[i]
            values.append(value)
        yield tuple(values)
