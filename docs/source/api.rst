API Reference
=============

.. py:currentmodule:: htmap


Map IDs
-------

The ``tag`` is the central organizing piece of data in HTMap.
Every map that you run produces a :class:`Map` which is connected to a unique ``tag``, a string that you must provide when you run the map.
A ``tag`` cannot be re-used until the associated map has been deleted.


Transient Mapping Functions
---------------------------

.. autofunction:: htmap.transient_map

.. autofunction:: htmap.transient_starmap


Persistent Mapping Functions
----------------------------

.. autofunction:: htmap.map

.. autofunction:: htmap.starmap

.. autofunction:: htmap.build_map


Map Builder
-----------

.. autoclass:: htmap.MapBuilder
   :members:

   .. automethod:: __call__

   .. automethod:: __len__


MappedFunction
--------------

A more convenient and flexible way to work with HTMap is to use the :func:`htmap` decorator to build a :class:`MappedFunction`.

.. autofunction:: htmap.mapped

.. autoclass:: htmap.MappedFunction
   :members:


Map
---

The :class:`Map` is your window into the status and output of your map.
Once you get a map result back from a map call,
you can use its methods to get the status of jobs,
change the properties of the map while its running,
pause, restart, or cancel the map,
and finally retrieve the output once the map is done.

The various methods that allow you to get and iterate over components will raise exceptions if something has gone wrong with your map:

* :class:`htmap.exceptions.MapComponentError` if a component experienced an error while executing.
* :class:`htmap.exceptions.MapComponentHeld` if a component was held by HTCondor, likely because an input file did not exist or the component used too much memory or disk.

The exception message will contain information about what caused the error.
See :ref:`error_handling` for more details on error handling.

.. autoclass:: htmap.Map
   :members:

   .. automethod:: __len__

   .. automethod:: __getitem__

.. autoclass:: htmap.ComponentStatus
   :members:


.. _error_handling:

Error Handling
--------------

Map components can generally encounter two kinds of errors:

* An exception occurred inside your function on the execute node.
* HTCondor was unable to run the map component for some reason.

The first kind will result in HTMap transporting a :class:`htmap.ComponentError` back to you,
which you can access via :meth:`htmap.Map.get_err`.
The :meth:`htmap.ComponentError.report()` method returns a formatted error report for your perusal.
:meth:`htmap.Map.error_reports` is a shortcut that returns all of the error reports for all of the components of your map.
If you want to access the error programmatically, you can grab it using :meth:`htmap.get_err`.

The second kind of error doesn't provide as much information.
The method :meth:`htmap.Map.holds` will give you a dictionary mapping components to their :class:`Hold`, if they have one.
:meth:`htmap.Map.hold_report` will return a formatted table showing any holds in your map.
The hold's ``reason`` attribute will tell you a lot about what HTCondor doesn't like about your component.

.. autoclass:: htmap.ComponentError
   :members:

.. autoclass:: htmap.Hold


MapOptions
----------

Map options are the equivalent of HTCondor's `submit descriptors <http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html>`_.
All HTCondor submit descriptors are valid map options **except** those reserved by HTMap for internal use (see below).

**Fixed options** are the most basic option.
The entire map will used the fixed option.
If you pass a single string as the value of a map option, it will become a fixed option.

**Variadic options** are options that are given individually to each component of a map.
For example, each component of a map might need a different amount of memory.
In that case you could pass a list to ``request_memory``, with the same number of elements as the number of inputs to the map.

**Inherited options** are given to a :class:`htmap.MappedFunction` when it is created.
Any maps made using that function can inherit these options.
Options that are passed in the actual map call override inherited options (excepting ``fixed_input_files``, see the note).
For example, if you know that a certain function always takes a large amount of memory, you could give it a large ``request_memory`` at the :class:`htmap.MappedFunction` level so that you don't have to do it for every individual map.
Additionally, default map options can be set globally via ``settings['MAP_OPTIONS.<option_name>'] = <option_value>``.

.. warning::

    Only certain options make sense as inherited options.
    For example, they shouldn't be variadic options.

    ``fixed_input_files`` has special behavior as an inherited option: they are *merged together* instead of overridden.

.. note::

    When looking at examples of raw HTCondor submit files, you may see submit descriptors that are prefixed with a ``+`` or a ``MY.``.
    Those options should be passed to :class:`htmap.MapOptions` via the ``custom_options`` keyword arguments.

.. autoclass:: htmap.MapOptions
   :members:


Checkpointing
-------------

.. autofunction:: htmap.checkpoint


Management
----------

These functions help you manage your maps.

.. autofunction:: htmap.status

.. autofunction:: htmap.tags

.. autofunction:: htmap.load

.. autofunction:: htmap.load_maps

.. autofunction:: htmap.remove

.. autofunction:: htmap.clean

.. autofunction:: htmap.force_remove

.. autofunction:: htmap.force_clean


Programmatic Status Messages
++++++++++++++++++++++++++++

These functions are useful for generating machine-readable status information.

.. autofunction:: htmap.status_json

.. autofunction:: htmap.status_csv

Transplant Installs
+++++++++++++++++++

These functions help you manage your transplant installs.

.. autofunction:: htmap.transplants

.. autoclass:: htmap.Transplant
   :members:

.. autofunction:: htmap.transplant_info

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

   These may be helpful when constructing fresh settings:

   * HTMap's base settings are available as ``htmap.BASE_SETTINGS``.
   * The settings loaded from ``~/.htmaprc`` are available as ``htmap.USER_SETTINGS``.

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


Version
-------

.. autofunction:: htmap.version

.. autofunction:: htmap.version_info
