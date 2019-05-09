FAQ
===

.. py:currentmodule:: htmap

.. _install:

How do I install HTMap?
-----------------------

On Unix/Linux systems, runing ``pip install htmap`` from the command line should suffice.
On Windows, you may need to install HTCondor itself to get access to the HTCondor Python bindings, and use the ``no-deps`` option with ``pip install``.
You only need to do this "submit-side", but you may also need to do some work to make sure your code will run execute-side: see :doc:`dependencies`.

* To get the latest development version of HTMap, run ``pip install git+https://github.com/htcondor/htmap.git`` instead.
* Run ``pip install git+https://github.com/htcondor/htmap.git@<branch>`` to install a specific branch.
* You may need to append ``--user`` to the ``pip`` command if you do not have permission to install packages directly into the Python you are using.

I'm getting a weird error from ``cloudpickle.load``?
----------------------------------------------------

You probably have a version mismatch between the submit and execute locations.
See the "Attention" box near the top of :doc:`dependencies`.

If you are using custom libraries, always import them before trying to load any output from maps that use them.

