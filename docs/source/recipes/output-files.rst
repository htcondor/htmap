.. py:currentmodule:: htmap

Output Files
------------

If the "output" of your map function is a file, HTMap's
basic functionality will not be sufficient for you.
As a toy example, consider a function which takes a string and a number, and
writes out a file containing that string repeated that number of times, with
a space between each repetition.
The file itself will be the output of our function.

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

There's no sign of our output file!
(A function with no ``return`` statement implicitly returns ``None``.)

We need to tell HTMap that we are producing an output file.
We can do this by adding a call to an HTMap hook function in our mapped function
after we create the file:

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
at the given paths back to the submit machine for us.
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

.. _transferring-output-to-other-places:

Transferring Output to Other Places
-----------------------------------

You may need to transfer output to places that are not the submit machine.
HTMap can arrange this for you using the ``output_remaps`` feature of
:class:`MapOptions` in combination with :class:`TransferPath` to specify
the destination of the output files.

In the below example, we have a function ``move_file`` that just tells
HTMap to transfer whatever input it is given.
We give the path to an input file stored in a S3 bucket named ``my-bucket`` on
some S3 server we can access, with some file (created and placed in the bucket
ahead of time) named ``in.txt``.
Our goal is to get that file back into the bucket, but renamed ``out.txt``.
To do so, we also create an ``output_file`` destination, and tell HTMap to
"remap" the output transfer via the ``output_remaps`` argument of
:class:`MapOptions`.

.. code-block:: python

    def move_file(input_path):
        htmap.transfer_output_files(input_path)


    bucket = htmap.TransferPath(
        "my-bucket", protocol="s3", location="s3-server.example.com"
    )
    input_file = bucket / "in.txt"
    output_file = bucket / "out.txt"

    print(input_file)  # TransferPath(path='my-bucket/in.txt', protocol='s3', location='s3-server.example.com')
    print(output_file)  # TransferPath(path='my-bucket/out.txt', protocol='s3', location='s3-server.example.com')

    map = htmap.map(
        move_file,
        [input_file],
        map_options=htmap.MapOptions(
            request_memory="128MB",
            request_disk="1GB",
            output_remaps=[{input_file.name: output_file}],
        ),
    )


After letting the map run, the output file will be in the bucket, and no
output file will have been sent back to the submit node
(i.e., ``m.output_files[0]`` will be an empty directory).
