Development Environment
=======================

.. py:currentmodule:: htmap

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
If you pass nothing, it will run ``bash`` with no arguments, giving you a shell
to work in.


Running the Test Suite
----------------------

The fastest way to run the test suite is to run ``pytest`` inside the
development container with multiple workers:

.. code:: shell

   $ ./dr
   # ...
   mapper@161b6af91d72:~/htmap$ pytest

``pytest -n 4`` seems to be a good number for laptops:

.. code:: shell

   mapper@161b6af91d72:~/htmap$ pytest -n 4

See `pytest-xdist <https://pypi.org/project/pytest-xdist/>`_ for more details.
The test suite is very slow when run serially; we highly recommend running
with a large number of workers (on a moderately-powerful desktop it seemed to
saturate around 10).


Building the Docs
-----------------

HTMap's documentation is served by `Read the Docs <https://readthedocs.org/>`_,
which builds the docs as well.
The docs are built automatically on each commit to master.

It can be helpful to build the docs locally during development.
We use ``sphinx-autobuild`` to serve the documentation via a local webserver
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
``binder/test.sh`` script from the repository root
(i.e., not from inside the development container):

.. code:: shell

   $ ./binder/run.sh

It will give you a link to enter into your web browser that will land you in the
same Jupyter environment you would get on Binder.

The ``binder/edit.sh`` script will do the same, but also bind-mount the
tutorials into the container so that they can be edited in the Jupyter environment.

When preparing a release, run ``binder/exec.sh`` and commit the results into
the repository.
