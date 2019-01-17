Development Environment
=======================

.. py:currentmodule:: htmap

HTMap's test suite relies on a properly set-up environment.
The simplest way to get that environment is to use the Dockerfile at the top of the repository source tree.
Also included is a bash script named ``dr`` (**d**\ ocker **r**\ un) that will let you quickly build and execute commands in the container.

.. attention::

    The Docker container is not linked to the host filesystem in real-time: if you make changes (either on the host or inside the container), you need to rebuild the Docker container!
    It should be fast because everything but the very last step can be cached by Docker.

Anything you pass to ``dr`` will be executed inside the container.
The initial working directory is the ``htmap`` repository inside the container, which has been editable-installed.
If you pass nothing, it will run ``pytest`` with no arguments.
Pass ``bash`` to get a shell.

The fastest way to run the test suite is to run ``pytest`` with multiple workers.
``pytest -n 10`` seems to be a good number.
See `pytest-xdist <https://pypi.org/project/pytest-xdist/>`_ for more details.
