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

    The ``dr`` script bind-mounts your local copy of the repository into the container.
    Any edits you make outside the container will be reflected inside (and vice versa).

Anything you pass to ``dr`` will be executed inside the container.
The initial working directory is the ``htmap`` repository inside the container.
If you pass nothing, it will run ``bash`` with no arguments, giving you a shell
to work in.


Running the Test Suite
----------------------

The fastest way to run the test suite is to run ``pytest`` inside the
development container with multiple workers.
``pytest -n 4`` seems to be a good number for laptops.
See `pytest-xdist <https://pypi.org/project/pytest-xdist/>`_ for more details.


Building the Docs
-----------------

HTMap's documentation is served by `Read the Docs <https://readthedocs.org/>`_,
which builds the docs as well.
However, it can be helpful to build the docs locally during development.
From inside the development container, run `docs/autobuild.sh` inside the
development container.
Note the startup message: ignore the link that `sphinx-autobuild` gives you,
and instead go to http://127.0.0.1:8000 to see the built documentation.


Binder Integration
------------------

HTMap's tutorials can be served via `Binder <https://mybinder.org/>`_.
The tutorials are run inside a specialized Docker container
(not the development container).
To test whether the Binder container is working properly, run the
``binder/test.sh`` script from the repository root
(i.e., not from inside the development container).
It will give you a link to enter into your web browser that will land you in the
same Jupyter environment you would get on Binder.
The ``binder/edit.sh`` will do the same, but also bind-mount the tutorials into
the container so that they can be edited in the Jupyter environment.
