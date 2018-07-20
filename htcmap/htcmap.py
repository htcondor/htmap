from typing import Any, Tuple, Iterable, Dict, Union, Optional, List

from pathlib import Path
import time
import hashlib
import itertools
from copy import deepcopy

import htcondor
import cloudpickle

HTCMAP_DIR = Path.home() / '.htcmap'


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
    mapper = htcmap(func)
    return mapper.map(args, **kwargs)


def productmap(func, *args, **kwargs) -> 'MapResult':
    mapper = htcmap(func)
    return mapper.productmap(*args, **kwargs)


def starmap(func, args, kwargs) -> 'MapResult':
    mapper = htcmap(func)
    return mapper.starmap(args, kwargs)


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
    def __init__(self, mapper: 'HTCMapper', clusterid: Optional[int], hashes: List[str]):
        self.mapper = mapper
        self.clusterid = clusterid
        self.hashes = hashes

        self.len = len(self.hashes)

    def __len__(self):
        return self.len

    def __getitem__(self, item: Union[int, str]) -> Any:
        if isinstance(item, int):
            item = self.hashes[item]

        path = self.mapper.outputs_dir / f'{item}.out'
        while not path.exists():
            time.sleep(1)

        return load(path)

    def __iter__(self) -> Iterable[Any]:
        for h in self.hashes:
            path = self.mapper.outputs_dir / f'{h}.out'
            while not path.exists():
                time.sleep(1)
            yield load(path)

    def iter_with_inputs(self) -> Iterable[Tuple[Any, Any]]:
        for h in self.hashes:
            input_path = self.mapper.inputs_dir / f'{h}.out'
            output_path = self.mapper.inputs_dir / f'{h}.out'
            while not output_path.exists():
                time.sleep(1)
            yield load(input_path), load(output_path)

    def iter_as_available(self) -> Iterable[Any]:
        paths = {self.mapper.outputs_dir / f'{h}.out' for h in self.hashes}
        while len(paths) > 0:
            for path in paths:
                if not path.exists():
                    continue
                with path.open(mode = 'rb') as file:
                    paths.remove(path)
                    yield cloudpickle.load(file)
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
        return f'{self.__class__.__name__}(htcmap = {self.mapper}, clusterid = {self.clusterid})'


class HTCMapper:
    def __init__(self, func, name, submit_descriptors = None):
        self.func = func
        self.name = name
        self.submit_descriptors = submit_descriptors or {}

        self.job_dir = HTCMAP_DIR / name
        self.inputs_dir = self.job_dir / 'inputs'
        self.outputs_dir = self.job_dir / 'outputs'
        self.job_logs_dir = self.job_dir / 'job_logs'
        self.cluster_logs_dir = self.job_dir / 'cluster_logs'

        for path in (
            self.job_dir,
            self.inputs_dir,
            self.outputs_dir,
            self.job_logs_dir,
            self.cluster_logs_dir,
        ):
            path.mkdir(parents = True, exist_ok = True)

        self.fn_path = self.job_dir / 'fn.pkl'
        if not self.fn_path.exists():
            save(self.func, self.fn_path)

    def __repr__(self):
        return f'{self.__class__.__name__}(name={self.name}, func={self.func})'

    def __call__(self, *args, **kwargs):
        # todo: this should also store in inputs/ and outputs/
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

        submit_dict = dict(
            jobbatchname = self.name,
            executable = str(Path(__file__).parent / 'run' / 'run.sh'),
            arguments = '$(Item)',
            log = str(self.cluster_logs_dir / '$(ClusterId).log'),
            output = str(self.job_logs_dir / '$(Item).output'),
            error = str(self.job_logs_dir / '$(Item).error'),
            should_transfer_files = 'YES',
            when_to_transfer_output = 'ON_EXIT',
            request_cpus = '1',
            request_memory = '100MB',
            request_disk = '5GB',
            transfer_input_files = ','.join([
                'http://proxy.chtc.wisc.edu/SQUID/karpel/htcmap.tar.gz',
                str(Path(__file__).parent / 'run' / 'run.py'),
                str(self.inputs_dir / '$(Item).in'),
                str(self.fn_path),
            ]),
            transfer_output_remaps = '"' + ';'.join([
                f'$(Item).out={self.outputs_dir / "$(Item).out"}',
            ]) + '"',
        )
        sub = htcondor.Submit(submit_dict)

        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            submit_result = sub.queue_with_itemdata(txn, 1, iter(new_hashes))

        return MapResult(
            mapper = self,
            clusterid = submit_result.cluster(),
            hashes = hashes,
        )

    def clean(self):
        for path in itertools.chain(self.inputs_dir.iterdir(), self.outputs_dir.iterdir()):
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
                if not num_active:
                    return
                iterators[i] = itertools.repeat(fills[i])
                value = fills[i]
            values.append(value)
        yield tuple(values)
