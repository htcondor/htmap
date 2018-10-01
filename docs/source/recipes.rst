Recipes
=======

.. py:currentmodule:: htmap

.. highlight:: python


.. _cleanup-after-force-removal:

Cleanup After Force Removal
---------------------------

If you use :func:`htmap.force_remove` or :func:`htmap.force_clean` you may end up with dangling map jobs.
These maps jobs are in the cluster's queue, but since you force-removed your map, you don't have a way to reconnect to them from inside HTMap.
You'll need to use the command line HTCondor tools ``condor_q`` and ``condor_rm`` to clean them up.
