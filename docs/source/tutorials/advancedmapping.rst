Advanced Mapping
================

.. py:currentmodule:: htmap

.. highlight:: python


Starmap
-------

Map Builders
------------

Mapped Functions
----------------

.. code-block:: python

    import htmap

    @htmap.htmap
    def double(x):
        return 2 * x

    result = double.map('dbl', range(10))

    print(list(result))  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

