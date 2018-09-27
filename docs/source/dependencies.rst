Dependency Management
=====================

.. py:currentmodule:: htmap

.. highlight:: python

Dependency management for Python programs is a thorny issue.
HTMap provides several methods for ensuring that your dependencies are available for your map components.

HTMap requires that the execute location can execute a Python script using a Python install that has the module ``cloudpickle`` installed.

.. attention::

    HTMap can transfer inputs and outputs between different versions of Python 3, but it can't magically make features from later Python versions available.
    For example, if you run Python 3.6 submit-side you can use f-strings in your code.
    But if you use Python 3.5 execute-side, your code will hit syntax errors because f-strings we're added until Python 3.6.

    Try to always use a latest version of Python everywhere.
    Failing that, use a later version of Python execute-side than submit-side.


Assume Dependencies are Present
-------------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    PYTHON_DELIVERY = "assume"

At runtime:

.. code-block:: python

    htmap.settings['PYTHON_DELIVERY'] = 'assume'

In this mode, HTMap assumes that a Python installation with all Python dependencies is already present.
Additional dependencies can still be delivered via :class:`MapOptions`.

.. note::

    When using this delivery method, HTMap will discover Python using this shebang:

    .. code-block:: bash

        #!/usr/bin/env python3


Run Inside a Docker Container
-----------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    PYTHON_DELIVERY = "docker"

    [DOCKER]
    IMAGE = "<repository>/<image>:<tag>"

At runtime:

.. code-block:: python

    htmap.settings['PYTHON_DELIVERY'] = 'docker'
    htmap.settings['DOCKER.IMAGE'] = "<repository>/<image>:<tag>"

In this mode, HTMap will run inside a Docker image that you provide.
Remember that this Docker image needs to have the ``cloudpickle`` module installed.
Because of limitations in HTCondor, your Docker image needs to be pushed back to `Docker Hub <https://hub.docker.com/>`_ to be usable.

For example, a very simple Dockerfile that can be used with HTMap is

.. code-block:: docker

    FROM python:latest

    RUN pip install --no-cache-dir cloudpickle

This would create a Docker image with the latest version of Python and ``cloudpickle`` installed.
From here you could install more Python dependencies, or add more layers to account for other dependencies.
Of course, you could also add the ``pip install`` line to your own image.

.. note::

    When using this delivery method, HTMap will discover Python using this shebang:

    .. code-block:: bash

        #!/usr/bin/env python3

.. note::

    The default Docker image is ``continuumio/anaconda3:latest``, which is using Python 3.5 under the hood.
    It comes with ``cloudpickle`` and many other packages pre-installed.

Transplant Existing Python Install
----------------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    PYTHON_DELIVERY = "transplant"

At runtime:

.. code-block:: python

    htmap.settings['PYTHON_DELIVERY'] = 'transplant'

If you are running HTMap from a standalone Python install (like an Anaconda installation), you can use this delivery mechanism to transfer a copy of your entire Python install.
All locally-installed packages (including ``pip -e`` installs) will be available.

.. note::

    The first time you run a map after installing/removing packages, you will need to wait while HTMap re-zips your installation.
    Subsequent maps will use the cached version.

    HTMap uses ``pip`` to check whether the cached Python is current, so make sure that ``pip`` is installed in your Python.

.. warning::

    This mechanism does not work with system Python installations (which you shouldn't be using anyway!).
