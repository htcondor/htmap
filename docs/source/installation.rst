.. _install:

Installation
============

* On Unix/Linux systems, running ``pip install htmap`` from the command line
  should suffice.
* On Windows, there's an added dependency of HTCondor (to get access to the
  HTCondor Python bindings). After that, use the ``pip install --no-deps``.

Most of the tutorials will run on Binder.

Basic usage only requires installation of HTMap "submit-side". Anything more
complex with HTMap like checkpointing or file transfers will require
installation on the execute nodes. For more information and to ensure your code
will run execute-side see :doc:`dependencies`.

To get the latest development version of HTMap, run ``pip install
git+https://github.com/htcondor/htmap.git`` instead.

You may need to append ``--user`` to the ``pip`` command if you do not have
permission to install packages directly into the Python you are using.
