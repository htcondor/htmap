API Reference
=============

.. py:currentmodule:: htmap


Map IDs
-------

The ``map_id`` is the central organizing piece of data in HTMap.
Every map that you run produces a :class:`MapResult` which is connected to a unique ``map_id``, a string that you must provide when you run the map.
A ``map_id`` cannot be re-used until the associated map has been deleted.


Mapping Functions
-----------------

.. autofunction:: htmap.map

.. autofunction:: htmap.starmap

.. autofunction:: htmap.build_map

.. autoclass:: htmap.MapBuilder
   :members:

.. note::

    The environment variable HTMAP_ON_EXECUTE is set to ``'1'`` while map components are executing out on the cluster.
    This can be useful if you need to switch certain behavior on whether you're running your function locally or not.

MappedFunction
--------------

A more convenient and flexible way to work with HTMap is to use the :func:`htmap` decorator to build an :class:`MappedFunction`.

.. autofunction:: htmap.htmap

.. autoclass:: htmap.MappedFunction
   :members:


MapResult
---------

The :class:`MapResult` is your window into the status and output of your map.
Once you get a map result back from a map call,
you can use its methods to get the status of jobs,
change the properties of the map while its running,
pause, restart, or cancel the map,
and finally retrieve the output once the map is done.

.. autoclass:: htmap.MapResult
   :members:


MapOptions
----------

**Fixed options** are the most basic option.
The entire map will used the fixed option.

**Variadic options** are options that are given individually to each component of a map.
For example, each component of a map might need a different amount of memory.
In that case you could pass a list to ``request_memory``, with the same number of elements as the number of inputs to the map.

**Inherited options** are given to a :class:`htmap.MappedFunction` when it is created.
Any maps made using that function can inherit these options.
Options that are passed in the actual map call override inherited options (excepting ``fixed_input_files``, see the note).
For example, if you know that a certain function always takes a large amount of memory, you could give it a large ``request_memory`` at the :class:`htmap.MappedFunction` level so that you don't have to do it for every individual map.
Additionally, default keyword arguments can be set globally via ``settings['MAP_OPTIONS.<option_name>'] = <option_value>``.

.. warning::

    Only certain options make sense as inherited options.
    For example, they shouldn't be variadic options.

    ``fixed_input_files`` has special behavior as an inherited option: they are *merged together* instead of overridden.

.. note::

    When looking at examples of raw HTCondor submit files, you may see submit descriptors that are prefixed with a ``+`` or a ``MY.``.
    Those options should be passed to :class:`htmap.MapOptions` via the ``custom_options`` keyword arguments.

.. autoclass:: htmap.MapOptions
   :members:


Management
----------

These functions help you manage your maps.

.. autofunction:: htmap.status

.. autofunction:: htmap.recover

.. autofunction:: htmap.map_ids

.. autofunction:: htmap.map_results

.. autofunction:: htmap.remove

.. autofunction:: htmap.clean

.. autofunction:: htmap.force_remove

.. autofunction:: htmap.force_clean


Settings
--------

HTMap exposes configurable settings through ``htmap.settings``, which is an instance of the class :class:`htmap.settings.Settings`.
This settings object manages a lookup chain of dictionaries.
The settings object created during startup contains two dictionaries.
The lowest level contains HTMap's default settings, and the next level up is constructed from a settings file at ``~/.htmaprc``.
If that file does not exist, an empty dictionary is used instead.
The file should be formatted in `TOML <https://github.com/toml-lang/toml>`_.

Alternate settings could be stored in other files or constructed at runtime.
HTMap provides tools for saving, loading, merging, prepending, and appending settings to each other.
Each map is search in order, so earlier settings can flexibly override later settings.

.. warning::
   To entirely replace your settings, do **not** do ``htmap.settings = <new settings object>``.
   Instead, use the :meth:`htmap.settings.Settings.replace` method.
   Replacing the settings by assignment breaks the internal linking between the settings objects and its dependencies.

.. hint::
   HTMap's base settings are available as ``htmap.BASE_SETTINGS``.
   The settings loaded from ``~/.htmap.toml`` are available as ``htmap.USER_SETTINGS``.
   These may be helpful when constructing fresh settings.

.. autoclass:: htmap.settings.Settings
   :members:

Logging
-------

HTMap exposes a `standard Python logging hierarchy <https://docs.python.org/3/library/logging.html>`_ under the logger named ``'htmap'``.
Logging configuration can be done by any of the methods described `in the documentation <https://docs.python.org/3/howto/logging.html#configuring-logging>`_.

Here's an example of how to set up basic console logging:

.. code-block:: python

    import logging
    import sys

    logger = logging.getLogger('htmap')
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream = sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    logger.addHandler(handler)

After executing this code, you should be able to see HTMap log messages as you tell it to do things.

.. warning::

    The HTMap logger is not available in the context of the executing map function.
    Trying to use it will probably raise exceptions.

Exceptions
----------

.. automodule:: htmap.exceptions
   :members:

