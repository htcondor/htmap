.. _dependency-management:

Dependency Management
=====================

.. py:currentmodule:: htmap

Dependency management for Python programs is a thorny issue.
HTMap provides several methods for ensuring that your dependencies are available for your map components.

HTMap requires that the execute location can execute a Python script using a Python install that has the module ``cloudpickle`` installed.

.. attention::

    HTMap can transfer inputs and outputs between different versions of Python 3, but it can't magically make features from later Python versions available.
    For example, if you run Python 3.6 submit-side you can use f-strings in your code.
    But if you use Python 3.5 execute-side, your code will hit syntax errors because f-strings were not added until Python 3.6.

    Try to always use the latest version of Python everywhere.
    Failing that, it's probably better to use a later version of Python execute-side than submit-side.


Assume Dependencies are Present
-------------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    DELIVERY_METHOD = "assume"

At runtime:

.. code-block:: python

    htmap.settings['DELIVERY_METHOD'] = 'assume'

In this mode, HTMap assumes that a Python installation with all Python dependencies is already present.
This will almost surely require some additional setup by your HTCondor pool's administrators.

Additional dependencies can still be delivered via :class:`MapOptions`.

.. note::

    When using this delivery method, HTMap will discover Python using this shebang as whatever user HTCondor runs your job as:

    .. code-block:: bash

        #!/usr/bin/env python3


Run Inside a Docker Container
-----------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    DELIVERY_METHOD = "docker"

    [DOCKER]
    IMAGE = "<repository>/<image>:<tag>"

At runtime:

.. code-block:: python

    htmap.settings['DELIVERY_METHOD'] = 'docker'
    htmap.settings['DOCKER.IMAGE'] = "<repository>/<image>:<tag>"

In this mode, HTMap will run inside a Docker image that you provide.
Remember that this Docker image needs to have the ``cloudpickle`` module installed.
The default Docker image is `continuumio/anaconda3:latest <https://hub.docker.com/r/continuumio/anaconda3/>`_, which is based on Python 3.5 and has many useful packages pre-installed.

If you want to use your own Docker image, just change the ``'DOCKER.IMAGE'`` setting.
Because of limitations in HTCondor, your Docker image needs to be pushed back to `Docker Hub <https://hub.docker.com/>`_ to be usable.
For example, a very simple Dockerfile that can be used with HTMap is

.. code-block:: docker

    FROM python:latest

    RUN pip install --no-cache-dir cloudpickle

This would create a Docker image with the latest version of Python and ``cloudpickle`` installed.
From here you could install more Python dependencies, or add more layers to account for other dependencies.
Of course, you could also add the ``pip install`` line to your own image.

.. warning::

    This delivery mechanism will only work if your HTCondor pool supports Docker jobs!
    If it doesn't, you'll need to talk to your pool administrators or use a different delivery mechanism.

.. note::

    When using this delivery method, HTMap will discover Python inside the container using this shebang:

    .. code-block:: bash

        #!/usr/bin/env python3


Transplant Existing Python Install
----------------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    DELIVERY_METHOD = "transplant"

At runtime:

.. code-block:: python

    htmap.settings['DELIVERY_METHOD'] = 'transplant'

If you are running HTMap from a standalone Python install (like an Anaconda installation), you can use this delivery mechanism to transfer a copy of your entire Python install.
All locally-installed packages (including ``pip -e`` installs) will be available.

For advanced transplant functionality, see :ref:`transplant-settings`.

.. note::

    The first time you run a map after installing/removing packages, you will need to wait while HTMap re-zips your installation.
    Subsequent maps will use the cached version.

    HTMap uses ``pip`` to check whether the cached Python is current, so make sure that ``pip`` is installed in your Python.

.. warning::

    This mechanism does not work with system Python installations (which you shouldn't be using anyway!).
