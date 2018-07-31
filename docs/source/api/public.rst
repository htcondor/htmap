Public API
==========

.. py:currentmodule:: htcmap

.. highlight:: python


HTCMapper
---------

The most powerful and flexible way to work with HTCMap is to use the :func:`htcmap` decorator to build an `HTCMapper`.
The mapper can distribute (i.e., map) function calls out over an HTCondor cluster.

.. autofunction:: htcmap.htcmap

.. autoclass:: htcmap.HTCMapper
   :members:

Module-Level Shortcut Functions
-------------------------------

These functions provide module-level shortcuts to the mapping methods of :class:`HTCMapper`.
You can call these functions on your function, along with your inputs, to get back a :class:`MapResult` without having to see the :class:`HTCMapper` in the middle.

.. autofunction:: htcmap.map

.. autofunction:: htcmap.starmap

.. autofunction:: htcmap.productmap

.. autofunction:: htcmap.build_job

MapResult
---------

.. autoclass:: htcmap.MapResult
   :members:

Settings
--------

.. autoclass:: htcmap.settings.Settings
   :members:


Exceptions
----------

.. automodule:: htcmap.exceptions
   :members: HTCMapException, MissingSetting, HashNotInResult, OutputNotFound, NoResultYet, TimeoutError

