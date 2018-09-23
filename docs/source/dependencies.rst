Dependency Management
=====================

.. py:currentmodule:: htmap

.. highlight:: python

Dependency management for Python programs is a thorny issue.
HTMap provides several methods for ensuring that your dependencies are available for your map components.

HTMap requires that the execute location can execute a Python script as an executable with the shebang

.. code-block:: bash

    #!/usr/bin/env python3

and that the Python that the shebang finds has the module ``cloudpickle`` installed.

Assume Dependencies are Present
-------------------------------

In your ``~/.htmaprc`` file:

.. code-block:: bash

    PYTHON_DELIVERY = "assume"

At runtime:

.. code-block:: python

    htmap.settings['PYTHON_DELIVERY'] = 'assume'

In this mode, HTMap assumes that any dependencies, including the Python install, are already present.
Additional dependencies can still be delivered via :class:`MapOptions`.

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
