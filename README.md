# htcmap

[![Documentation Status](https://readthedocs.org/projects/htmap/badge/?version=latest)](https://htcmap.readthedocs.io/en/latest/?badge=latest)
[![Build Status](https://travis-ci.org/JoshKarpel/htmap.svg?branch=master)](https://travis-ci.org/JoshKarpel/htcmap)
[![codecov](https://codecov.io/gh/JoshKarpel/htmap/branch/master/graph/badge.svg)](https://codecov.io/gh/JoshKarpel/htcmap)

## Quick Install

1. Get the miniconda installer via `wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh` and run it.
   Install wherever you want.
1. Set up your `PATH` so that it includes `miniconda3/bin` (the `bin` directory in that fresh Python install, wherever it is), at least temporarily.
   `which python` and `which pip` should both point to executables in that `bin` directory.
1. `pip install cloudpickle toml htcondor==8.7.9rc3` - install `cloudpickle`, `toml`, and the HTCondor bindings.

There are two ways to install `htmap` itself.
The first is better if you want to do some local development, or want to switch between different branches.
The second is more convenient if you don't.

1. `git clone https://github.com/JoshKarpel/htmap` somewhere.
1. `pip install -e path/to/htmap/` - do a local, editable install of `htcmap`.
   Any changes to the local repo will be reflected when you re-import the module.

Or:

1. `pip install git+https://github.com/JoshKarpel/htcmap`, to install directly from GitHub.

To update `htcmap`, just do a `git pull` inside that directory (or `git checkout <branch>` to switch branches, etc.).


## Basic Usage

Fun example:
```python
from htmap import htmap

@htmap
def double(x):
    return 2 * x

job = double.map([5, 'foo', 10, 'bar'])

for result in job:
    print(result)
```
This should block while waiting for the jobs to finish in order.
As they finish, the results from each should be printed.

