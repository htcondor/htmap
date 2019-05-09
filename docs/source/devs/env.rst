Development Environment
=======================

.. py:currentmodule:: htmap

HTMap's test suite relies on a properly set-up environment.
The simplest way to get that environment is to use the Dockerfile in ``docker/Dockerfile``.
Also included is a bash script named ``dr`` (**d**\ ocker **r**\ un) in the repository root
that will let you quickly build and execute commands in the container.

.. attention::

    **The Docker container is not linked to the host filesystem in real-time**:
    if you make changes (either on the host or inside the container), you need to rebuild the Docker container!
    It should be fast because everything but the very last step can be cached by Docker.

Anything you pass to ``dr`` will be executed inside the container.
The initial working directory is the ``htmap`` repository inside the container, which has been editable-installed.
If you pass nothing, it will run ``pytest`` with no arguments.
Pass ``bash`` to get a shell.

The fastest way to run the test suite is to run ``pytest`` with multiple workers.
``pytest -n 10`` seems to be a good number.
See `pytest-xdist <https://pypi.org/project/pytest-xdist/>`_ for more details.

Binder Integration
------------------

HTMap's tutorials can be served via `Binder <https://mybinder.org/>`_.
To test whether Binder integration is working properly, run the ``binder/test`` script from the repository root.
It will give you a link to enter into your web browser that will land you in the same Jupyter Lab environment you would get on Binder.

Building the Docs
-----------------

HTMap's documentation is served by `Read the Docs <https://readthedocs.org/>`_, which builds the docs as well.
However, it can be helpful to build the docs locally.
To do so, you'll need to do an editable install of ``htmap`` (``pip install -e .`` from the repository root), and then also install the extra documentation requirements (``pip install -r requirements_dev.txt``).
You can then go into the ``docs`` directory and run ``make html`` to build the docs.

For rapid development, I recommend ``pip install sphinx-autobuild`` and running ``sphinx-autobuild source build/html`` from the ``docs`` directory.
