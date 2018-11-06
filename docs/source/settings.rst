Settings
========

.. py:currentmodule:: htmap

HTMap's settings are controlled by a global object which you can access as ``htmap.settings``.
For more information on how this works, see :class:`htmap.settings.Settings`.

Users can provide custom default settings by putting them in a file in their home directory named ``.htmaprc``.
The file is in `TOML format <https://github.com/toml-lang/toml>`_.

HTMap's settings are organized into groupings based on TOML headers.
The settings inside each group are discussed in the following sections.

At runtime, settings can be found via dotted paths that correspond to the section heads.
Here, I'll give the dotted paths - if they're in the file instead, each dot is a header.

Here is an example ``.htmaprc`` file:

.. code-block:: toml

    DELIVERY_METHOD = "docker"

    [MAP_OPTIONS]
    REQUEST_MEMORY = "250MB"

    [DOCKER]
    IMAGE = "python:latest"

The equivalent runtime Python commands to set those settings would be

.. code-block:: python

    import htmap

    htmap.settings['DELIVERY_METHOD'] = 'docker'
    htmap.settings['MAP_OPTIONS.REQUEST_MEMORY'] = '250MB'
    htmap.settings['DOCKER.IMAGE'] = 'python:latest'


Settings
--------

These are the top-level settings.
They do not belong to any header.

``HTMAP_DIR`` - the path to the directory to use as the HTMap directory.
If not given, defaults to ``~/.htmap``.

``MAPS_DIR_NAME`` - the name of the directory inside the ``HTMAP_DIR`` to store map information in.

``DELIVERY_METHOD`` - the name of the delivery method to use.
The different delivery methods are discussed in :ref:`dependency-management`.
Defaults to ``docker``.

``WAIT_TIME`` - how long to wait between polling for component statuses, files existing, etc.
Measured in seconds.
Defaults to ``1`` (1 second).

MAP_OPTIONS
+++++++++++

Any settings in this section are passed to every :class:`MapOption` as keyword arguments.


HTCONDOR
++++++++

``SCHEDD`` - the address of the HTCondor scheduler (see :class:`htcondor.Schedd`).
If set to ``None``, HTMap discovers the local scheduler automatically.


DOCKER
++++++

These settings control how the ``docker`` delivery method works.

``IMAGE`` - the path to the Docker image to run components with.
Defaults to ``'continuumio/anaconda3:latest'``.
If the environment variable ``HTMAP_DOCKER_IMAGE`` is set, that will be used as the default instead.

.. _transplant-settings:

TRANSPLANT
++++++++++

These settings control how the ``transplant`` delivery method works.

``DIR`` - the path to the directory where the zipped Python install will be cached.
Defaults to a subdirectory of ``HTMAP_DIR`` named ``transplants``.

``ALTERNATE_INPUT_PATH`` - a string that will be used in the HTCondor ``transfer_input_files`` option **instead of** the local file path.
If set to ``None``, the local path will be used (the default).
This can be used to override the default file transfer mechanism.

``ASSUME_EXISTS`` - if set to ``True``, assume that the zipped Python install already exists.
Most likely, you will need to set ``ALTERNATE_INPUT_PATH`` to an existing zipped install.
Defaults to ``False``.
