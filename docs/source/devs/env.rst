Development Environment
=======================

.. py:currentmodule:: htmap

Repository Setup
----------------

You can get HTMap's source code by cloning the git repository:
``git clone https://github.com/htcondor/htmap``.
If you are planning on submitting a pull request, you should instead
clone your own
`fork <https://help.github.com/en/github/getting-started-with-github/fork-a-repo>`_
of the repository.

After cloning the repository,
install the development dependencies using your Python package manager.
If you are using ``pip``, you would run
``pip install -e .[tests,docs]`` from the repository root.
The dependencies (development and otherwise) are listed in ``setup.cfg``.

.. warning::

    The HTCondor Python bindings are currently only available via PyPI on Linux.
    On Windows you must install HTCondor itself to get them.
    On Mac, you're out of luck.
    Install ``pre-commit`` manually, then use the development container to run
    the test suite/build the documentation.

One of the dependencies you just installed is ``pre-commit``. ``pre-commit``
runs a series of checks whenever you try to commit. You should "install" the
pre-commit hooks by running ``pre-commit install`` in the repository root.
You can run the checks manually at any time by running ``pre-commit``.

**Do not commit to the repository before running** ``pre-commit install`` **!**


Development Container
---------------------

HTMap's test suite relies on a properly set-up environment.
The simplest way to get that environment is to use the Dockerfile in
``docker/Dockerfile`` to produce a **development container**.
The repository includes a bash script named ``dr`` (**d**\ ocker **r**\ un)
in the repository root that will let you quickly build and execute commands
in a development container.

.. attention::

    The ``dr`` script bind-mounts your local copy of the repository into the
    container.  Any edits you make outside the container will be reflected
    inside (and vice versa).

Anything you pass to ``dr`` will be executed inside the container.
By default (i.e., if you pass nothing) you will get a ``bash`` shell.
The initial working directory is the ``htmap`` repository inside the container.


Running the Test Suite
----------------------

The best way to run the test suite is to run ``pytest`` inside the
development container:

.. code:: shell

   $ ./dr
   # ...
   mapper@161b6af91d72:~/htmap$ pytest

The test suite can be executed in parallel by passing the ``-n`` option.
``pytest -n 4`` seems to be a good number for laptops, while desktops can
probably handle ``-n 10``.
See `pytest-xdist <https://pypi.org/project/pytest-xdist/>`_ for more details
on parallel execution.
The test suite is very slow when run serially; we highly recommend running
with a large number of workers.

See `the pytest docs <https://docs.pytest.org/>`_
or run ``pytest --help`` for more information on
`pytest` itself.


Building the Docs
-----------------

HTMap's documentation is served by `Read the Docs <https://readthedocs.org/>`_,
which builds the docs as well.
The docs are deployed automatically on each commit to master,
so they can be updated independently of a version release for minor adjustments.

It can be helpful to build the docs locally during development.
We recommend using ``sphinx-autobuild`` to serve the documentation via a local webserver
and automatically rebuild the documentation when changes are made to the
package source code or the documentation itself.
To run the small wrapper script we have written around ``sphinx-autobuild``,
from inside or outside the development container run,

.. code:: shell

   $ ./dr
   # ...
   mapper@161b6af91d72:~/htmap$ docs/autobuild.sh
   NOTE: CONNECT TO http://127.0.0.1:8000 NOT WHAT SPHINX-AUTOBUILD TELLS YOU
   # trimmed; visit URL above

Note the startup message: ignore the link that ``sphinx-autobuild`` gives you,
and instead go to http://127.0.0.1:8000 to see the built documentation.


Binder Integration
------------------

HTMap's tutorials can be served via `Binder <https://mybinder.org/>`_.
The tutorials are run inside a specialized Docker container
(not the development container).
To test whether the Binder container is working properly, run the
``binder/run.sh`` script from the repository root
(i.e., not from inside the development container):

.. code:: shell

   $ ./binder/run.sh

It will give you a link to enter into your web browser that will land you in the
same Jupyter environment you would get on Binder.

The ``binder/edit.sh`` script will do the same, but also bind-mount the
tutorials into the container so that they can be edited in the Jupyter environment.

When preparing a release, run ``binder/exec.sh`` and commit the results into
the repository.
