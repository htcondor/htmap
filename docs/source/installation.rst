.. _install:

Installation
============

* On Unix/Linux systems, running ``pip install htmap`` from the command line
  should suffice.
* On Windows, there's an added dependency of HTCondor (to get access to the
  HTCondor Python bindings). After that, use the ``pip install --no-deps``.

The introductory tutorials can be run on Binder,
requiring no setup on your part.

Basic usage only requires installation of HTMap "submit-side". Anything more
advanced like checkpointing or output file transfers will require
installation on the execute nodes. For more information and to ensure your code
will run correctly execute-side see :doc:`dependencies`.

You may need to append ``--user`` to the ``pip`` command if you do not have
permission to install packages directly into the
Python installation you are using.
Recent versions of ``pip`` will do this automatically when necessary.
