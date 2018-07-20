# htcmap


## Quick Install

1. Get the miniconda installer via `wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh` and run it.
   Install wherever you want.
1. Set up your `PATH` so that it includes `miniconda3/bin` (the `bin` directory in that fresh Python install, wherever it is), at least temporarily.
   `which python` and `which pip` should both point to executables in that `bin` directory.
1. `pip install cloudpickle htcondor` - install cloudpickle and the HTCondor bindings.
1. `git clone https://github.com/JoshKarpel/htcmap` somewhere.
1. `pip install -e path/to/htcmap/` - do a local, editable install of htcmap so that you can run it on the submit node.

Fun example:
```python
from htcmap import htcmap

@htcmap()
def double(x):
    return 2 * x

job = double.map([5, 'foo', 10, 'bar'])

for result in job:
    print(result)
```
This should block while waiting for the jobs to finish in order.
As they finish, the results from each should be printed.


## Things That Don't Exist Yet But Definitely Should and Other Problems

* The function should provide keyword arguments with sensible defaults to allow more customization.
  Or, just let it take `**kwargs` and chainmap them into the submit dictionary.
* The job object should capture more information about the jobs, allow querying of that information through the Python bindings, etc.
* Still need to ship Python out to execute nodes.
  I think there may be a clever way to zip up and ship the Python install that the user is actually running their interpreter from (`sys.executable` is the path to the `python` executable), so at least we can capture their installed packages.
  I can't think of any way to avoid this...
* Catch and return pickled exceptions if something goes wrong?
* What if the wrapper script needs additional setup?
  For example, mine needs some flags set, but that could maybe happen through a submit file option.
  But others may need more complicated setup.
