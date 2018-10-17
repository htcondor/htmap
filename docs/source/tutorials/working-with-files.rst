.. _tutorial-working-with-files:

Working with Files
==================

.. py:currentmodule:: htmap


Sometimes your maps will have extra input that would be inconvenient to move into Python.
This input is often in the form of external data files that need to be analyzed or compared.

As a simple example, let's say we have some plain text files each, each with one word per line.
We want to compare each of these files to a "master" file and return a list that tells us whether each line matches the same line number in the other file.
One possible implementation looks like this:

.. code-block:: python

    import htmap

    import itertools

    @htmap.mapped
    def compare_files(test_file: str, master_file: str = None) -> list:
        line_comparisons = []
        with open(test_file, mode = 'r') as test, open(master_file, mode = 'r') as master:
            for test_line, master_line in zip(test, master):
                line_comparisons.append(test_line == master_line)

        return line_comparisons

As described above, this function returns a list of booleans, one for each line in the shorter of the two files.
If the lines are the same, the boolean is ``True``.
If they're not the same, it's ``False``.

Here are some text files that we can use to test this:

.. code-block:: text

    # master.txt
    foo
    bar
    baz

    # a.txt
    bing
    bar
    bonk

    # b.txt
    foo
    quop
    quip

    # c.txt
    quaz
    kin
    baz

Create these files, ideally in the current working directory of your Python process (use ``Path.cwd()`` to figure out where that is).
If they're not there, you'll just need to provide absolute paths to them later, instead of the relative paths I have here.
Assuming you do have relative paths, we can make them pretty easily using a list comprehension:

.. code-block:: python

    test_files = ['{}.txt'.format(s) for s in 'abc']

Now we're ready to make our map.
To tell HTMap about our extra input files, we need to give the map call a :class:`htmap.MapOptions` instance.
The :class:`htmap.MapOptions` carries the extra information about our map through keyword arguments given to it.

.. code-block:: python

    map_options = htmap.MapOptions(
        fixed_inputs_files = ['master.txt'],  # the **path** to the master file
        input_files = test_files,  # the **paths** to the test files
    )

    result = compare_files.map(
        'compare',
        files,  # these are the actual inputs to the function: the **names** of the test files
        master_file = 'master.txt',  # this keyword argument is applied to every component of the map
        map_options = map_options,  # pass in the map options we built above
    )

The distinction between paths and names is important.
In :class:`htmap.MapOptions` we need to provide **local paths** so that HTMap can find the requested files on the computer you're submitting from.
But when the function actually runs, those files will just be next to in a scratch directory, so the function arguments should just be the names of the files.
The detailed rules about where transferred files end up is rather complicated - see `the HTCondor manual <http://research.cs.wisc.edu/htcondor/manual/latest/SubmittingaJob.html#x17-380002.5.9>`_ for more details.

Iterating over the output, we see what we expected to see:

.. code-block:: python

    for r in result:
        print(r)

    # [False, True, False]
    # [True, False, False]
    # [False, False, True]


Providing input files for your maps is just one thing that :class:`htmap.MapOptions` can do.
For more information on them, see the next tutorial: :ref:`tutorial-map-options`.
