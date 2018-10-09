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

    print(filtered)  # [((0,), {}), ((2,), {}), ((4,), {}), ((6,), {}), ((8,), {})]


.. _groupby:

Groupby
-------

In the parlance of higher-order functions, HTMap only provides map.
Another higher-order function, groupby, is easy to implement once you have a map.
To mimic it we'll write a helper function that uses a :class:`collections.defaultdict` to construct a dictionary that collects inputs that have the same output, using the output as the key.

Here's a brief example: grouping integer by whether they are even or not.

.. code-block:: python

    import collections
    import htmap

    @htmap.htmap
    def is_even(x: int) -> bool:
        return x % 2 == 0

    def groupby(result):
        groups = collections.defaultdict(list)

        for input, output in result.iter_with_inputs():
            groups[output].append(input)

        return groups

    result = is_even.map('is_even', range(10))

    for group, elements in groupby(result).items():
        print(group, elements)

    # True [((0,), {}), ((2,), {}), ((4,), {}), ((6,), {}), ((8,), {})]
    # False [((1,), {}), ((3,), {}), ((5,), {}), ((7,), {}), ((9,), {})]


.. _cleanup-after-force-removal:

Cleanup After Force Removal
---------------------------

If you use :func:`htmap.force_remove` or :func:`htmap.force_clean` you may end up with dangling map jobs.
These maps jobs are in the cluster's queue, but since you force-removed your map, you don't have a way to reconnect to them from inside HTMap.
You'll need to use the command line HTCondor tools ``condor_q`` and ``condor_rm`` to clean them up.
