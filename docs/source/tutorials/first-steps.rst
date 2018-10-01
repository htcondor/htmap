First Steps
===========

.. py:currentmodule:: htmap

.. highlight:: python

Suppose you've been given the task of writing a function that doubles numbers.
This is hardly a challenge:

.. code-block:: python

    def double(x):
        return 2 * x

Now suppose that, for some reason, you want to double *a lot* of numbers.
So many numbers that you can't bear to do all the work on your own computer.
It takes days to multiply all the numbers, and if your program crashes halfway through, you lose all of of your progress and have to start over.
You're losing sleep, and your boss is breathing down your neck because they need those numbers doubled *now*.

Luckily, you remember that you have access to an HTCondor high-throughput computing pool.
Since each of your function calls is isolated from all the others, the computers in the pool don't need to talk to each other at all, and you can achieve a huge speedup.
The pool can run your code on hundreds or thousands of computers simultaneously, storing the inputs and outputs for you and recovering from individual errors gracefully.
It's the perfect solution.

The problem is: *how do you get your code running in the pool?*

With HTMap, it's like this:

.. code-block:: python

    import htmap

    result = htmap.map('dbl', double, range(10))

    print(list(result))  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

It may take some time for the ``print`` to run.
During that time, the individual **components** of your **map** are being run out on the cluster.
Once they all finish, you'll get the list of numbers back.
These function outputs are identical to what you would get from running the function locally.

The ``result`` that was returned by :func:`htmap.map` is a :class:`htmap.MapResult`.
It gives us a window into the map as it is running, and lets us use the output once the map is finished.
We'll explore what it can do for us later.
For now, let's return to how we created the map in the first place.

The first argument to :func:`htmap.map` is a **map ID**.
These are unique strings that you must provide to HTMap to keep track of your maps.
If you lose your :class:`htmap.MapResult`, you can **recover** it:

.. code-block:: python

    recovered_result = htmap.recover('dbl')

    print(recovered_result)  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

This ``print`` should happen immediately: the map has already run, so it's output is stored for you locally.

Maps can be recovered from an entirely different Python interpreter session as well.
Suppose you close Python and go on vacation.
You come back and you want to look at your map again, but you've forgotten what you called it.
Just ask HTMap for a list of your map IDs:

.. code-block:: python

    print(htmap.map_ids())  # ('dbl',)

Ok, well, technically it was a tuple, but we'll have to live with it.
HTMap can also print a pretty table showing the status of your maps:

.. code-block:: python

    print(htmap.status())

    #  Map ID │ Held │ Idle │ Run │ Done │   Data
    # ────────┼──────┼──────┼─────┼──────┼─────────
    #   dbl   │  0   │  0   │  0  │  10  │ 20.0 KB
    # ────────┴──────┴──────┴─────┴──────┴─────────

Map IDs are *unique*: if we try to create another map with the same map ID we just used, it will fail:

.. code-block:: python

    new_result = htmap.map('dbl', double, range(10))

    # htmap.exceptions.MapIdAlreadyExists: the requested map_id dbl already exists (recover the MapResult, then either use or delete it).

As the error message indicates, if we just wanted to get `'dbl'` back, we need to :func:`htmap.recover` it instead.
If we wanted to make a totally new map with the same ID, we need to remove the old map first:

.. code-block:: python

    result.remove()

:meth:`htmap.MapResult.remove` deletes all traces of the map, and it can never be recovered.
Be careful when using it!

As a shortcut, we could have also done

.. code-block:: python

    new_result = htmap.map('dbl', double, range(10), force_overwrite = True)

The ``force_overwrite`` keyword tells HTMap to remove the map with that ID (if it exists) before creating the new one.

Where to Next?
--------------

Now that you've seen the core of HTMap, you may want to start thinking about

1. I want to learn about how to use files as input data: :ref:`tutorial-working-with-files`.
2. I want to learn about how to tell the pool what resources my maps need: :ref:`tutorial-map-options`.
3. I want to learn about how to use more powerful mappers: :ref:`tutorial-advanced-mapping`.
