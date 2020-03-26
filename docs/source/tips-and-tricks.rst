Tips & Tricks
=============

:doc:`tips-and-tricks/htcondor`
   How to set up an environment for development and testing.

:doc:`tips-and-tricks/api`
   API tricks on functional programming with HTMap.

Note: the environment variable ``HTMAP_ON_EXECUTE`` is set to ``'1'`` while map
components are executing out on the cluster.  This can be useful if you need to
switch certain behavior on or off depending whether you're running your
function locally or not.

.. toctree::
   :maxdepth: 2
   :hidden:

   tips-and-tricks/api
   tips-and-tricks/htcondor
