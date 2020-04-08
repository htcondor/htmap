FAQ
===

.. py:currentmodule:: htmap

.. _install:

How do I install HTMap?
-----------------------

On Unix/Linux systems, running ``pip install htmap`` from the command line should suffice.
On Windows, you may need to install HTCondor itself to get access to the HTCondor Python bindings, and use the ``no-deps`` option with ``pip install``.
You only need to do this "submit-side", but you may also need to do some work to make sure your code will run execute-side: see :doc:`dependencies`.

* To get the latest development version of HTMap, run ``pip install git+https://github.com/htcondor/htmap.git`` instead.
* Run ``pip install git+https://github.com/htcondor/htmap.git@<branch>`` to install a specific branch.
* You may need to append ``--user`` to the ``pip`` command if you do not have permission to install packages directly into the Python you are using.

Is it possible to use Dask with HTCondor?
-----------------------------------------

`Dask Distributed`_ is a lightweight library for distributed Python computation.
Dask Distributed has familiar APIs, is declarative and supports more complex
scheduling than map/filter/reduce.

`Dask-Jobqueue`_ present a wrapper for HTCondor clusters through their
`HTCondorCluster`_. After `HTCondorCluster`_ is used, Dask can be used as
normal or on your own machine. This is common with other cluster managers too:
Dask-Jobqueue also wraps SLURM, SGE, PBS and LSF clusters, and Dask Distributed
can wrap Kubernetes and Hadoop clusters. This interface will not use HTMap.

.. _Dask-Jobqueue: https://jobqueue.dask.org/en/latest/
.. _HTCondorCluster: https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.HTCondorCluster.html#dask_jobqueue.HTCondorCluster
.. _Dask Distributed: https://distributed.dask.org/

I'm getting a weird error from ``cloudpickle.load``?
----------------------------------------------------

You probably have a version mismatch between the submit and execute locations.
See the "Attention" box near the top of :doc:`dependencies`.

If you are using custom libraries, always import them before trying to load any output from maps that use them.

I'm getting an error about a job being held. What should I do?
--------------------------------------------------------------

Your code likely encountered an error during remote execution. Briefly, try
viewing the standard error (``stderr``) with HTMap, either via the CLI or API.
Details can be found in :doc:`tutorials` and :doc:`tutorials/error-handling`.
