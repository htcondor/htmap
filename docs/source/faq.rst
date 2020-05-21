FAQ
===

.. py:currentmodule:: htmap

How do I abort a job?
---------------------

For example, say you mistakenly launched a map tagged ``foo``,
but now want to abort/cancel it, fix some input parameters, then relaunch it.

The right CLI command is ``htmap remove foo``, or the HTMap function
:func:`~htmap.remove`. This mirrors the HTCondor API and will remove the job
from the job scheduler regardless of state (running, idle, held, etc).

.. _successful-jobs:

How do I only process completed jobs?
-------------------------------------

Let's say you submitted 10,000 long-running jobs, and 99.9% of these jobs have
finished successfully. You'd like to get the results from the successful jobs,
and save the results to disk without have to wait for the 10 remaining jobs
slow jobs.

The right function to use is :func:`~htmap.Map.components_by_status`. It can
filter out the successful jobs and process those. See the
:func:`~htmap.Map.components_by_status` documentation for an example usage.

Is it possible to use Dask with HTCondor? How does it compare with HTMap?
-------------------------------------------------------------------------

HTMap provides a transparent interface to the underlying HTCondor
behavior, allowing for features like using HTCondor file transfer and taking
advantage of the rich HTCondor job model.
HTMap does need to be running through the entire duration of your computation.

Dask can spawn its distributed workers on an HTCondor pool.
By doing this you get access to Dask's features, but not HTCondor's.
Dask will need to be running through the entire duration of your computation.

**You should choose the appropriate option for your use case.**

`Dask Distributed`_ is a lightweight library for distributed Python computation.
Dask Distributed has familiar APIs, is declarative and supports more complex
scheduling than map/filter/reduce.

`Dask-Jobqueue`_ present a wrapper for HTCondor clusters through their
`HTCondorCluster`_. After `HTCondorCluster`_ is used, Dask can be used as
normal or on your own machine. This is common with other cluster managers too:
Dask-Jobqueue also wraps SLURM, SGE, PBS and LSF clusters, and Dask Distributed
can wrap Kubernetes and Hadoop clusters.

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
