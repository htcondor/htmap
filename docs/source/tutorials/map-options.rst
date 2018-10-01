.. _tutorial-map-options:

Map Options
===========

.. py:currentmodule:: htmap

.. highlight:: python

HTMap makes fairly conservative choices about the resources required by your map components.
If your function needs a lot of resources, such as memory or disk space, you will need to communicate this to HTMap.

:class:`htmap.MapOptions` also accepts arbitrary keyword arguments.
These keyword arguments are interpreted as arbitrary HTCondor submit file options, as described `here <http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html>`_.
