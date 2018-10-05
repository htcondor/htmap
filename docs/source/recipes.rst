Recipes
=======

.. py:currentmodule:: htmap


.. _filter:

Filter
------

In the parlance of higher-order functions, HTMap only provides map.
Another higher-order function, filter, is easy to implement once you have a map.
To mimic it we create a map with a boolean output, and use :func:`htmap.MapResult.iter_with_inputs` inside a list comprehension to filter the inputs using the outputs.

Here's a brief example: checking whether integers are even.

.. code-block:: python

    import htmap

    @htmap.htmap
    def is_even(x: int) -> bool:
        return x % 2 == 0

    result = is_even.map('is_even', range(10))

    filtered = [input for input, output in result.iter_with_inputs() if output]

    print(filtered)  # [0, 2, 4, 6, 8]


.. _cleanup-after-force-removal:

Cleanup After Force Removal
---------------------------

If you use :func:`htmap.force_remove` or :func:`htmap.force_clean` you may end up with dangling map jobs.
These maps jobs are in the cluster's queue, but since you force-removed your map, you don't have a way to reconnect to them from inside HTMap.
You'll need to use the command line HTCondor tools ``condor_q`` and ``condor_rm`` to clean them up.
