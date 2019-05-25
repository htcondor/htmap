.. py:currentmodule:: htmap

Output Files
------------

If the "output" of your map function is a file, HTMap's
basic functionality will not be sufficient for you.
As a toy example, consider a function which takes a string and a number, and
writes out a file containing that string repeated that number of times, with
a space between each repetition.
The file itself will be the output artifact of our function.

.. code-block:: python

    import htmap

    import itertools
    from pathlib import Path

    @htmap.mapped
    def repeat(string, number):
        output_path = Path('repeated.txt')

        with output_path.open(mode = 'w') as f:
            f.write(' '.join(itertools.repeat(string, number)))

This would work great locally, producing a file named ``repeated.txt`` in
the directory we ran the code from.
If this same code runs execute-side, the file will still be produced, but
HTMap won't know that we care about the file.
In fact, the map will appear to be spectacularly useless:

.. code-block:: python

    with repeat.build_map() as mb:
        mb('foo', 5)
        mb('wiz', 3)
        mb('bam', 2)

    repeated = mb.map

    print(list(repeated))
    # [None, None, None]

A function with no ``return`` statement implicitly returns ``None``.
There's no sign of our output file.

We need to tell HTMap that we are producing an output file.
We can do this by adding a call to an HTMap hook function in our mapped function:

.. code-block:: python

    import htmap

    import itertools
    from pathlib import Path

    @htmap.mapped
    def repeat(string, number):
        output_path = Path('repeated.txt')

        with output_path.open(mode = 'w') as f:
            f.write(' '.join(itertools.repeat(string, number)))

        htmap.transfer_output_files(output_path)  # identical, except for this line

The :func:`htmap.transfer_output_files` function tells HTMap to move the files
at the given paths back for us.
We can then access those files using the :attr:`Map.output_files` attribute,
which behaves like a sequence indexed by component numbers.
The elements of the sequence are :class:`pathlib.Path` pointing to the
directories containing the output files from each component, like so:

.. code-block:: python

    with repeat.build_map() as mb:
    mb('foo', 5)
    mb('wiz', 3)
    mb('bam', 2)

    repeated = mb.map

    for component, base in enumerate(repeated.output_files):
        path = base / 'repeated.txt'
        print(component, path.read_text())

    # 0 foo foo foo foo foo
    # 1 wiz wiz wiz
    # 2 bam bam

