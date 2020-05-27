.. _dependency-management:

Dependency Management
=====================

.. py:currentmodule:: htmap

Dependency management for Python programs is a thorny issue in general, and
running code on computers that you don't own is even thornier.
HTMap provides several methods for ensuring that the software that your code
depends on is available for your map components.
This could include other Python packages like ``numpy`` or ``tensorflow``, or
external software like ``gcc``.

There are two halves of the dependency management game.
The first is on "your" computer, which we call **submit-side**.
This could be your laptop running a personal HTCondor pool,
or an HTCondor "submit node" that you ``ssh`` to,
or whatever other way you access your HTCondor pool.
The other side is **execute-side**, which isn't really a single place:
it is all of the execute nodes in the pool that your map components might run on.

Submit-side dependency management can be handled using standard Python package
management tools.
We recommend using ``miniconda`` as your package manager
(https://docs.conda.io/en/latest/miniconda.html).

HTMap itself requires that execute-side can run a Python script using a Python
install that also has ``htmap`` installed.
That Python installation also needs whatever other packages your code needs to
run.
For example, if you ``import numpy`` in your code, you need to have ``numpy``
installed execute-side.

As mentioned above, HTMap provides several "delivery methods" for getting that
Python installation to the execute location.
The built-in delivery methods are

* ``docker`` - runs in a (possibly user-supplied) Docker container.
* ``singularity`` - runs in a (possibly user-supplied) Singularity container.
* ``shared`` - runs with the same Python installation used submit-side.
* ``assume`` - assumes that the dependencies have already been installed at
  the execute location.
* ``transplant`` - copy the submit-side Python installation to the execute
  location.

More details on each of these methods can be found below.

The default delivery method is ``docker``, with the default image
``htcondor/htmap-exec:<version>``,
where version will match the version of HTMap you are using submit-side.
If your pool can run Docker jobs and your Python code does not depend on any
custom packages
(i.e., you never import any modules that you wrote yourself),
this default behavior will likely work for you without requiring any changes.
See the section below on Docker if this isn't the case!

.. attention::

    HTMap can transfer inputs and outputs between different minor versions of Python 3, but it can't magically make features from later Python versions available.
    For example, if you run Python 3.6 submit-side you can use f-strings in your code.
    But if you use Python 3.5 execute-side, your code will hit syntax errors because f-strings were not added until Python 3.6.
    We don't actually test cross-version transfers though, and we recommend running exactly the same version of Python on submit and execute.

    HTMap **cannot** transfer inputs and outputs between different versions of ``cloudpickle``.
    Ensure that you have the same version of ``cloudpickle`` installed everywhere.

    If you see an exception on a component related to ``cloudpickle.load``, this is the most likely culprit.
    Note that you may need to manually upgrade/downgrade your submit-side or execute-side ``cloudpickle``.


Run Inside a Docker Container
-----------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    DELIVERY_METHOD = "docker"

    [DOCKER]
    IMAGE = "<repository>/<image>:<tag>"

At runtime:

.. code-block:: python

    htmap.settings["DELIVERY_METHOD"] = "docker"
    htmap.settings["DOCKER.IMAGE"] = "<repository>/<image>:<tag>"

In this mode, HTMap will run inside a Docker image that you provide.
Remember that this Docker image needs to have the ``htmap`` module installed.
The default Docker image is
`htcondor/htmap-exec <https://hub.docker.com/r/htcondor/htmap-exec/>`_,
which is based on Python 3 and has many useful packages pre-installed.

If you want to use your own Docker image, just change the ``'DOCKER.IMAGE'``
setting.
Your Docker image needs to be pushed back to
`Docker Hub <https://hub.docker.com/>`_
(or some other Docker image registry that your HTCondor pool can access)
to be usable.
For example, a very simple Dockerfile that can be used with HTMap is

.. code-block:: docker

    FROM python:3

    RUN pip install --no-cache-dir htmap

This would create a Docker image with the latest versions of Python 3 and
``htmap`` installed.
From here you could install more Python dependencies, or add more layers to
account for other dependencies.

.. attention::

    More information on building Docker images for use with HTMap can be found
    in the :doc:`recipes/docker-image-cookbook`.


.. warning::

    This delivery mechanism will only work if your HTCondor pool supports
    Docker jobs!
    If it doesn't, you'll need to talk to your pool administrators or use a
    different delivery mechanism.


Run Inside a Singularity Container
----------------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    DELIVERY_METHOD = "singularity"

    [SINGULARITY]
    IMAGE = "<image>"

At runtime:

.. code-block:: python

    htmap.settings["DELIVERY_METHOD"] = "singularity"
    htmap.settings["SINGULARITY.IMAGE"] = "<image>"

In this mode, HTMap will run inside a Singularity image that you provide.
Remember that this Singularity image needs to have the ``cloudpickle`` module
installed.

Singularity can also use Docker images.
Specify a Docker Hub image as
``htmap.settings['SINGULARITY.IMAGE'] = "docker://<repository>/<image>:<tag>"``
to download a Docker image from DockerHub and automatically use it as a
Singularity image.

For consistency with Docker delivery, the default Singularity image is
`docker://continuumio/anaconda3:latest <https://hub.docker.com/r/continuumio/anaconda3/>`_,
which has many useful packages pre-installed.

If you want to use your own Singularity image, just change the
``'SINGULARITY.IMAGE'`` setting.

.. warning::

    This delivery mechanism will only work if your HTCondor pool supports
    Singularity jobs!
    If it doesn't, you'll need to talk to your pool administrators or use a
    different delivery mechanism.


.. note::

    When using this delivery method, HTMap will discover ``python3`` on the
    system ``PATH`` and use that to run your code.


.. warning::

    This delivery method relies on the directory ``/htmap/scratch`` either
    existing in the Singularity image, or Singularity being able to run
    with ``overlayfs``.
    If you get a ``stderr`` message from Singularity about a bind mount
    directory not existing, that's the problem.


Run With a Shared Python Installation
-------------------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    DELIVERY_METHOD = "shared"

At runtime:

.. code-block:: python

    htmap.settings["DELIVERY_METHOD"] = "shared"

In this mode, HTMap will run your components using the same interpreter being
used submit-side.
This requires that that the submit-side Python interpreter be
"visible" from the execute location, which is usually done in one of two ways:

1. The execute location **is** the submit location
   (i.e., they are the same physical computer).
2. The Python installation is stored on a shared filesystem, such that submit
   and execute can both see the same file paths.

Either way, the practical requirement to use this delivery method is that the
path to the Python interpreter
(i.e., ``python -c "import sys, print(sys.executable)"``)
is the same both submit-side and execute-side.


Assume Dependencies are Present
-------------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    DELIVERY_METHOD = "assume"

At runtime:

.. code-block:: python

    htmap.settings["DELIVERY_METHOD"] = "assume"

In this mode, HTMap assumes that a Python installation with all Python
dependencies is already present.
This will almost surely require some additional setup by your HTCondor
pool's administrators.


Transplant Existing Python Install
----------------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    DELIVERY_METHOD = "transplant"

At runtime:

.. code-block:: python

    htmap.settings["DELIVERY_METHOD"] = "transplant"

If you are running HTMap from a standalone Python install
(like an Anaconda installation),
you can use this delivery mechanism to transfer a copy of your entire Python
install.
All locally-installed packages (including ``pip -e`` "editable" installs) will
be available.

For advanced transplant functionality, see :ref:`transplant-settings`.

.. note::

    The first time you run a map after installing/removing packages,
    you will need to wait while HTMap re-zips your installation.
    Subsequent maps will use the cached version.

    HTMap uses ``pip`` to check whether the cached Python is current, so make
    sure that ``pip`` is installed in your Python.

.. warning::

    This mechanism does not work with system Python installations
    (which you shouldn't be using anyway!).

.. note::

    When using the transplant method the transplanted Python installation will
    be used to run the component,
    regardless of any other Python installations that might exist execute-side.
