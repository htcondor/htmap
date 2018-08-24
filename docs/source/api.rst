API Reference
=============

.. py:currentmodule:: htmap

.. highlight:: python


HTMapper
--------

The most powerful and flexible way to work with HTMap is to use the :func:`htmap` decorator to build an `HTMapper`.
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

.. autoclass:: htmap.settings.Settings
   :members:


Exceptions
----------

.. automodule:: htmap.exceptions
   :members:

