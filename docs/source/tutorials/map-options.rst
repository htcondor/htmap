.. _tutorial-map-options:

Map Options
===========

.. py:currentmodule:: htmap


HTMap makes fairly conservative choices about the resources required by your map components.
If your function needs a lot of resources, such as memory or disk space, you will need to communicate this to HTMap.

Suppose we need to transfer a huge input file that we need to read into memory, so we need a lot of memory and disk.
This will be the same for every single map we make with this function.
HTMap lets us provide default :class:`htmap.MapOptions` when we create the mapped function, by passing them to the :func:`htmap.htmap` decorator:
We'll request 1 GB of RAM, 20 GB of disk space, and send our input file.

.. code-block:: python

    import htmap

    @htmap.mapped(
        request_memory = '1GB',
        request_disk = '20GB',
        fixed_input_files = ['huge_input_file.blob'],
    )
    def my_important_function(x):
        ...

Now whenever we make a map from the :class:`htmap.MappedFunction`, those options will be applied.
If we include :class:`htmap.MapOptions` on those individual calls, those options will individually override the ones stored on the :class:`MappedFunction`.
The one exception is ``fixed_input_files``: the lists will be merged instead of overridden.

:class:`htmap.MapOptions` also accepts arbitrary keyword arguments, which are interpreted as arbitrary HTCondor submit file options, as described `here <http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html>`_.
