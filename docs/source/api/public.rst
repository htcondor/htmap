Public API
==========

.. py:currentmodule:: htmap

.. highlight:: python


HTMapper
---------

The most powerful and flexible way to work with HTMap is to use the :func:`htmap` decorator to build an `HTMapper`.
The mapper can distribute (i.e., map) function calls out over an HTCondor cluster.

.. autofunction:: htmap.htmap

.. autoclass:: htmap.HTMapper
   :members:

.. autoclass:: htmap.JobBuilder
   :members:

Module-Level Shortcut Functions
-------------------------------

These functions provide module-level shortcuts to the mapping methods of :class:`HTMapper`.
You can call these functions on your function, along with your inputs, to get back a :class:`MapResult` without having to see the :class:`HTMapper` in the middle.

.. autofunction:: htmap.map

.. autofunction:: htmap.starmap

.. autofunction:: htmap.productmap

.. autofunction:: htmap.build_job

MapResult
---------

.. autoclass:: htmap.MapResult
   :members:

Settings
--------

.. autoclass:: htmap.settings.Settings
   :members:


Exceptions
----------

.. automodule:: htmap.exceptions
   :members: HTMapException, MissingSetting, HashNotInResult, OutputNotFound, NoResultYet, TimeoutError

