Development Environment
=======================

.. py:currentmodule:: htmap

.. highlight:: python

HTMap's test suite relies on a properly set-up environment.
This page describes how to set up such an environment.

Installing HTCondor
-------------------

First, the test suite needs to interact with a real HTCondor pool, either a personal pool or a real pool out in the world.
For maximum control and to make sure tests run quickly, we advise using a personal pool without much other work on it.
Installation instructions can be found `here <https://research.cs.wisc.edu/htcondor/instructions/>`_.


Installing Python
-----------------

Second, you'll need to install Python.
Your system likely already comes with some version of Python installed on it.
Although HTMap can be installed in the system Python, there are two reasons not to:

1. It's not good to mess with the system Python install in general, because you might break your computer.
2. Some of HTMap's features won't work when run from a system Python install, so you won't be able to run all of the tests.

We recommend running HTMap from a `miniconda <https://conda.io/miniconda.html>`_ installation, or some other kind of virtual environment.


Cloning and Installing HTMap
----------------------------

Clone the HTMap repository from ``https://github.com/htcondor/htmap`` to whatever location you'd like.
Activate your virtual environment and run ``pip install -e path/to/htmap/``.
This performs an "editable" installation: you'll be able to ``import htmap`` from any Python process (as if you had installed it normally), but any changes you make in the repository will be reflected the next time you import.

.. note::

    While developing, you may find the :func:`importlib.reload` function useful.
    For example, to re-import HTMap from inside a running REPL, run

    .. code-block:: python

        >>> import htmap
        >>> import importlib
        >>> importlib.reload(htmap)


Getting Delivery Mechanisms Working
-----------------------------------

Transplant
++++++++++

``transplant`` delivery should work out-of-the-box, assuming you are running from a standalone Python install as described above.

Assume
++++++

For ``assume`` delivery, you need to ensure that ``cloudpickle`` is installed in whatever Python the user that your jobs run as sees with the shebang ``#!/usr/bin/env python3``.
Likely, that Python will be the system Python.
On a personal HTCondor, the user is probably yourself (but without your ``.bashrc`` environment).
Since we don't want to install things in the system Python globally, we can do a user-specific install by doing something like ``/usr/bin/pip3 install --user cloudpickle``, where the path should point to the ``pip`` for the appropriate Python.
You need to run this command as the user that HTCondor uses, which may involve becoming that user using `sudo su` if it's not you.

Docker
++++++

For ``docker`` delivery, you need to make sure that your pool can host Docker jobs.
If you're starting from scratch, this will probably involve `installing Docker <https://docs.docker.com/install/#supported-platforms>`_.
Then set up HTCondor for Docker integration as described in the `HTCondor manual's administrators section <http://research.cs.wisc.edu/htcondor/manual/>`_.

Running the Tests
-----------------

Now we're ready to run the test suite!
HTMap's test suite is written using pytest, and therefore requires a few extra dependencies.
They are listed in the ``requirements_dev.txt`` file in the top level of the repository - a ``pip install -r requirements_dev.txt`` should do it.

Now just run ``pytest`` from inside the repository.
It should discover the tests and run them.
Hopefully they all pass!

The Python script ``covtest.py`` is also provided in the top level of the repository to facilitate quick coverage testing.
Run it like ``python covtest.py`` to see a coverage report in the terminal.
An HTML report is also written to a directory called ``covreport``, next to the script.
