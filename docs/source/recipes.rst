Recipes
=======

.. py:currentmodule:: htmap

.. highlight:: python

.. _wrapping_external_programs:

Wrapping External Programs
--------------------------

HTMap can only map Python functions, but you might need to call an external program on the execute node.
For example, you may need to use a particular Bash utility script, or run a piece of pre-compiled analysis software.
In cases like this, the Python standard library's `subprocess module <https://docs.python.org/3/library/subprocess.html>`_ can be used to communicate with those programs.

For example, suppose you need to call the Dubious Barology Lyricon (``dbl``) program, a pre-compiled C program that you have stored in your home directory at ``~/dbl``.
It takes a single integer argument, and "returns" a single integer by printing it to standard output.
So a call to ``dbl`` on the command line looks like

.. code-block:: bash

    $ dbl 4
    8

To use HTMap with ``dbl``, you could write a mapped function that looks something like

.. code-block:: python

    import subprocess
    import htmap

    @htmap.htmap(
        map_options = htmap.MapOptions(
            fixed_input_files = 'dbl',
        )
    )
    def dbl(x):
        process = subprocess.run(
            ['dbl', str(x)],
            stdout = subprocess.PIPE,  # use capture_output = True in Python 3.7+
        )

        if process.returncode != 0:
            raise Exception('call to dbl failed!')

        return_value = int(process.stdout)

        return return_value

You'll need to be careful with functions like this - check for failures in the programs you call, because HTMap will happily return nonsense if the call fails in some strange way.
If we do a map, we'll end up with the expected result:

.. code-block:: python

    result = dbl.map('dbl', range(10))

    print(list(result))  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

If you want to test this yourself, here's the Dubious Barology Lyricon (really a simple ``bash`` program):

.. code-block:: bash

    #!/usr/bin/env bash

    echo $((2 * $1))


.. _cleanup-after-force-removal:

Cleanup After Force Removal
---------------------------

When you use :func:`htmap.force_remove` or :func:`htmap.force_clean`, you may end up with dangling map jobs.
These maps jobs are in the cluster's queue, but since you force-removed your map, you don't have a way to reconnect to them from inside HTMap.
You'll need to use the command line HTCondor tools ``condor_q`` and ``condor_rm`` to clean them up.
