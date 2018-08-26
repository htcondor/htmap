API Reference
=============

.. py:currentmodule:: htmap

.. highlight:: python


HTMapper
--------

The most powerful and flexible way to work with HTMap is to use the :func:`htmap` decorator to build an :class:`HTMapper`.
The mapper can distribute (i.e., map) function calls out over an HTCondor cluster.

.. autofunction:: htmap.htmap

.. autoclass:: htmap.HTMapper
   :members:

.. autoclass:: htmap.MapBuilder
   :members:


MapResult
---------

.. autoclass:: htmap.MapResult
   :members:


Management
----------

These functions help you manage your maps.

.. autofunction:: htmap.clean

.. autofunction:: htmap.map_ids

.. autofunction:: htmap.status


Shortcut Functions
------------------

These are module-level shortcut functions which let you produce and recover :class:`MapResult`\s.

.. autofunction:: htmap.map

.. autofunction:: htmap.starmap

.. autofunction:: htmap.productmap

.. autofunction:: htmap.build_map

.. autofunction:: htmap.recover


Settings
--------

HTMap exposes configurable settings through `htmap.settings`, which is an instance of the class :class:`htmap.settings.Settings`.
This settings object manages a lookup chain of dictionaries.
The settings object created during startup contains two dictionaries.
The lowest level contains HTMap's default settings, and the second is constructed from a settings file at `~/.htmap.toml`.
If that file does not exist, an empty dictionary is used instead.
As you can guess from the extension, the file is be formatted in `TOML <https://github.com/toml-lang/toml>`_.

Alternate settings could be stored in other files or constructed at runtime.
HTMap provides tools for saving, loading, merging, prepending, and appending settings to each other.
Each map is search in order, so earlier settings can flexibly override later settings.

.. warning::
   To entirely replace your settings, do **not** do ``htmap.settings = <new settings object>``.
   Instead, use the :meth:`htmap.settings.Settings.replace` method.
   Replacing the settings by assignment breaks the internal linking between the settings objects and its dependencies.

.. autoclass:: htmap.settings.Settings
   :members:


Exceptions
----------

.. automodule:: htmap.exceptions
   :members:

