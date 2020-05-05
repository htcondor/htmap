Tips and Tricks
===============

.. py:currentmodule:: htmap


.. _cli-tips:

Separate Job Submission/Monitoring/Collection
---------------------------------------------

This is recommended because it's more interactive and more flexible: it doesn't
rely on the script being free of bugs on submission. Likewise, un-expected
errors can easily be adapted (such as hung jobs, etc).
This is most appropriate for medium- or long-running jobs.

The CLI is useful to monitor and modify ongoing jobs. Generally, in simple use
cases we recommend writing two or three scripts:

* A script for job submission (which is run once).
* A script for monitoring jobs (which is run many times).
* A script to collect results (which is a few times).

Each script uses these commands:

* Submission: HTMap's Python API is primarily used here, possibly through
  :func:`map`.
* Monitoring: CLI usage is heavy here, and the command ``htmap status`` is
  heavily used. If any of the jobs fail, it's easy to diagnose why with the
  additional commands.
* Collection: the completed jobs are collected (as mentioned in
  :ref:`successful-jobs`) and the results are written to disk/etc.

Use of the CLI is useful for debugging when any of the scripts produces an
error.


Use the CLI
-----------

Use of the CLI is recommended to go alongside separation of
submission/monitoring/collection as mentioned above. This section will provide
some useful commands.  This command shows the status of each job for various
tags:

.. code:: shell

   htmap status  # See info on each job, and various tags

This might indicate that 4 jobs in tag ``foo`` are completed and 2 are idle (or
waiting to be run).  These commands will show more information about individual
maps and map components:

.. code::

   htmap logs  # get path to log file; info here is useful for debugging
   htmap components foo  # view which component status for tag "foo"
   htmap errors foo # view all errors for tag "foo"
   htmap stdout foo 0  # view stdout for first component of tag "foo"
   htmap stderr foo 0  # view stdout for first component of tag "foo"
   htmap reasons foo  # get reasons for holding map "foo"

Some of the longer output is useful to pipe into ``less`` so it's easily
navigable and searchable. For example,

.. code:: shell

   htmap errors foo | less

To get help on ``less``, use the command ``man less`` or press ``h`` while in
``less``.

Full CLI documentation is at :ref:`cli`.

Conditional Execution on Cluster vs. Submit
-------------------------------------------

The environment variable ``HTMAP_ON_EXECUTE`` is set to ``'1'`` while map components are executing out on the cluster.
This can be useful if you need to switch certain behavior on or off depending whether you're running your function locally or not.


Functional programming
----------------------
.. _filter:

Filter
^^^^^^

In the parlance of higher-order functions, HTMap only provides map.
Another higher-order function, filter, is easy to implement once you have a map.
To mimic it we create a map with a boolean output, and use :func:`htmap.Map.iter_with_inputs` inside a list comprehension to filter the inputs using the outputs.

Here's a brief example: checking whether integers are even.

.. code-block:: python

    import htmap

    @htmap.mapped
    def is_even(x: int) -> bool:
        return x % 2 == 0

    result = is_even.map(range(10))

    filtered = [input for input, output in result.iter_with_inputs() if output]

    print(filtered)  # [((0,), {}), ((2,), {}), ((4,), {}), ((6,), {}), ((8,), {})]


.. _groupby:

Groupby
^^^^^^^

In the parlance of higher-order functions, HTMap only provides map.
Another higher-order function, groupby, is easy to implement once you have a map.
To mimic it we'll write a helper function that uses a :class:`collections.defaultdict` to construct a dictionary that collects inputs that have the same output, using the output as the key.

Here's a brief example: grouping integer by whether they are even or not.

.. code-block:: python

    import collections
    import htmap

    @htmap.mapped
    def is_even(x: int) -> bool:
        return x % 2 == 0

    def groupby(result):
        groups = collections.defaultdict(list)

        for input, output in result.iter_with_inputs():
            groups[output].append(input)

        return groups

    result = is_even.map(range(10))

    for group, elements in groupby(result).items():
        print(group, elements)

    # True [((0,), {}), ((2,), {}), ((4,), {}), ((6,), {}), ((8,), {})]
    # False [((1,), {}), ((3,), {}), ((5,), {}), ((7,), {}), ((9,), {})]
