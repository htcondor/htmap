FAQ
===

.. py:currentmodule:: htmap

.. _install:

How do I install HTMap?
-----------------------

.. caution::

    The instructions given below will eventually be true, but we're not on PyPI yet.
    For the moment you should install HTMap by running ``pip install git+https://github.com/htcondor/htmap.git``.

On Unix/Linux systems, ``pip install htmap`` should suffice.

* To get the latest development version of HTMap, run ``pip install git+https://github.com/htcondor/htmap.git`` instead.
* Run ``pip install git+https://github.com/htcondor/htmap.git@<branch>`` to install a specific branch.
* You may need to append ``--user`` to the ``pip`` command if you do not have permission to install packages directly into the Python you are using.

I'm getting a weird error from ``cloudpickle.load``?
----------------------------------------------------

You probably have a version mismatch between the submit and execute locations.
See the "Attention" box near the top of :doc:`dependencies`.

If you are using custom libraries, always import them before trying to load any output from maps that use them.

