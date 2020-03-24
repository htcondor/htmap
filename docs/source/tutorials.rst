Tutorials
=========

Basic Tutorials
---------------

.. attention::

    The most convenient way to go through these tutorials is through Binder, which requires no setup on your part: |binder|

.. |binder| image:: https://mybinder.org/badge_logo.svg
   :target: https://mybinder.org/v2/gh/htcondor/htmap/master?urlpath=lab/tree/tutorials/first-steps.ipynb


:doc:`tutorials/first-steps`
   If this is your first time using HTMap, start here!

:doc:`tutorials/basic-mapping`
   An introduction to the basics of HTMap.

:doc:`tutorials/working-with-files`
   Sending additional files with your maps.

:doc:`tutorials/map-options`
   How to tell the pool what to do with your map.

:doc:`tutorials/advanced-mapping`
   More (and better) ways to create maps.

:doc:`tutorials/error-handling`
   What do when something goes wrong.

Advanced Tutorials
------------------

`Note: these tutorial can not be run with Binder`

.. py:currentmodule:: htmap

:doc:`recipes/docker-image-cookbook`
   How to build HTMap-compatible Docker images.
   Yes, this single recipe is an entire cookbook!

:doc:`recipes/output-files`
   How to move arbitrary files back to the submit node.

:doc:`recipes/wrapping-external-programs`
   How to send input and output to an external (i.e., non-Python) program from inside a mapped function.

:doc:`recipes/checkpointing-maps`
   How to write a function that can continue from partial progress after being evicted.

:doc:`recipes/using-htmap-on-osg`
   How to use HTMap on the `Open Science Grid <https://opensciencegrid.org/>`_.


.. toctree::
   :maxdepth: 2
   :hidden:

   tutorials/first-steps
   tutorials/basic-mapping
   tutorials/working-with-files
   tutorials/map-options
   tutorials/advanced-mapping
   tutorials/error-handling
   recipes/docker-image-cookbook
   recipes/output-files
   recipes/wrapping-external-programs
   recipes/checkpointing-maps
   recipes/using-htmap-on-osg
