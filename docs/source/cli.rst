CLI Reference
=============

The CLI is useful to monitor and modify ongoing jobs.

Useful commands
---------------

.. code:: shell

   htmap --help  # View available commands

This will reveal some of these useful commands to view information:

.. code:: shell

   htmap status  # See info on each job, and various tags
   htmap clean --all  # remove all tags
   htmap logs  # get path to log file; info here is useful for debugging
   htmap components foo  # view which component status for tag "foo"
   htmap errors foo # view all errors for tag "foo"
   htmap stdout foo 0  # view stdout for first job of tag "foo"
   htmap stderr foo 0  # view stdout for first job of tag "foo"

Some of the longer output is useful to pipe into ``less`` so it's easily
navigable and searchable. For example,

.. code:: shell

   htmap errors foo | less

To get help on ``less``, use the command ``man less`` or press ``h`` while in
``less``.

It's also possible to edit/rerun jobs:

.. code:: shell

   htmap edit memory foo --unit GB 10  # change 10GB of memory for tag "foo"
   htmap edit disk foo --unit GB 10  # change to 10GB of disk for tag "foo"
   htmap rerun components foo 0  # rerun component 0 of tag "foo"


Full CLI documentation
----------------------

Here's the full documentation on all of the available commands:

.. click:: htmap.cli:cli
   :prog: htmap
   :show-nested:
