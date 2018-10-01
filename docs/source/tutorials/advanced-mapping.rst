.. _tutorial-advanced-mapping:

Advanced Mapping
================

.. py:currentmodule:: htmap

.. highlight:: python


Starmap
-------

The basic :func:`htmap.map` function can only handle functions that vary over a single positional argument.
The more general version is provided by :func:`htmap.starmap`, which handles arbitrary combinations of positional and keyword arguments.

.. code-block:: python

    def power(x, p = 1):
        return x ** p


    result = htmap.starmap(
        map_id = 'pow',
        func = power,
        args = [
            (1,),
            (2,),
            (3,),
        ],
        kwargs = [
            {'p': 1},
            {'p': 2},
            {'p': 3},
        ],
    )

    print(list(result))  # [1, 4, 27]

The syntax is a little awkward because we need to pass the positional and keyword arguments as tuples and dictionaries.
In reality, I recommend pre-building your arguments using generator or list comprehensions.
For example, to produce the above example, I would do

.. code-block:: python

    result = htmap.starmap(
        map_id = 'pow',
        func = power,
        args = ((x,) for x in range(1, 4)),
        kwargs = ({'p': p} for p in range(1, 4)),
    )

    print(list(result))  # [1, 4, 27]


Map Builders
------------

If you find :func:`htmap.starmap` inconvenient, you may prefer building your map using a :class:`htmap.MapBuilder`.
To get a :class:`htmap.MapBuilder`, use the :class:`htmap.build_map` function:

.. code-block:: python

    with htmap.build_map(map_id = 'pow_builder', func = power) as builder:
        for x in range(1, 4):
            builder(x, p = x)

    result = builder.result
    print(list(result))  # [1, 4, 27]

The advantage of the map builder is that we don't need to build intermediate lists of arguments.
We call the builder as if it was the function, and the builder caches the inputs and internally converts them into a :func:`htmap.starmap` call.
The map is created when the ``with`` block ends.


Mapped Functions
----------------

If you have a function that you want to create many maps from, you may find it convenient to build a :class:`htmap.MappedFunction` using the :func:`htmap.htmap` decorator:

.. code-block:: python

    import htmap

    @htmap.htmap
    def double(x):
        return 2 * x

    result = double.map('dbl', range(10))

    print(list(result))  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

The :class:`htmap.MappedFunction` has methods that provide shortcuts to :class:`htmap.map`, :class:`htmap.starmap`, and :class:`htmap.build_map` which automatically plug in the wrapped function.
Additionally, the :class:`htmap.MappedFunction` can carry a base :class:`htmap.MapOptions` that will be applied to all maps created from it (for more on :class:`htmap.MapOptions`, see :ref:`tutorial-map-options`).
