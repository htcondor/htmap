Innards
=======

.. py:currentmodule:: htmap

.. highlight:: python


Data Model
----------

Guiding Principles
++++++++++++++++++

* The only identifying piece of information about a map a user should ever need is a ``map_id``.
* Users should never have to directly interact with the filesystem to manipulate their maps.
* We should store as little state as possible in memory.
* Any state we do store should be duplicated on disk immediately.

Test Suite
----------

The HTMap test suite makes several assumptions about your system:
* That the Python bindings can discover a working HTCondor pool from your computer (for example, you could run a personal HTCondor).
* That a Python executable with ``cloudpickle`` installed is visible to the user that HTCondor uses to execute jobs.
