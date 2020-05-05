Using HTCondor with HTMap
=========================

Specification on exactly what "held", "idle", "running" and "completed" jobs
mean is detailed at the `HTCondor documentation on machine states`_.

.. _HTCondor documentation on machine states: https://htcondor.readthedocs.io/en/latest/admin-manual/policy-configuration.html#machine-states

Requesting commonly used resources
----------------------------------

HTCondor's default configuration can be limiting -- what if your job requires
more memory or more disk space? HTCondor can be configured to allow this, and
HTMap supports the configuration via :class:`~htmap.MapOptions`.

:class:`~htmap.MapOptions` accepts many of the same keys that `condor_submit`_
accepts.  Some of the more commonly requested keys are:

* ``request_memory``. Possible values are ``"1M`` for 1MB, ``"2GB"`` for 2GB of
  memory. If not specified, the HTCondor's defaults are accepted provided
  ``JOB_DEFAULT_REQUESTMEMORY`` is not set (one of the
  `configuration variables`_).
* ``request_cpus``. Possible values are ``"1"`` for 1 CPU.
* ``request_disk``. Possible values are ``"10GB"`` for 10GB, ``"1T"`` for 1
  terabytes.

These would be set with :class:`~htmap.MapOptions`. For example, this code
might be used:

.. code:: python

   options = ht.MapOptions(
       request_cpus="1",
       request_disk="10GB",
       request_memory="4GB",
   )
   ht.map(..., map_options=options)

When it's mentioned that "the option ``foo`` needs to be set" (possibly in a
submit file), this corresponds to adding the option in the appropriate
place in :class:`~MapOptions` (see the documentation for details).

.. _configuration variables: https://htcondor.readthedocs.io/en/latest/admin-manual/configuration-macros.html

GPUs
----

* For any GPU job, the option ``request_gpus`` needs to be set.
* Many GPU jobs are machine learning jobs. CHTC has a guide on "`Run Machine
  Learning Jobs on the HTC system`_".

There are some site-specific options. For example, CHTC has a guide on some of
these options "`Jobs that use GPUs`_" to run jobs on their `GPU Lab`_. Check
with your site's documentation to see if they have any GPU documentation.

.. _GPU Lab: http://chtc.cs.wisc.edu/gpu-lab
.. _Jobs that use GPUs: http://chtc.cs.wisc.edu/gpu-jobs
.. _Run Machine Learning Jobs on the HTC system: http://chtc.cs.wisc.edu/machine-learning-htc

Shell commands
--------------

Here are some shell HTCondor commands and their primary use:

* `condor_q`_: seeing the jobs submitted to the scheduler (aliased to
  :func:`htmap.status`)
* `condor_status`_: seeing resources the different machines have

The links go an HTML version of the man pages; their also visible with ``man``
(e.g., ``man condor_q``).  Here's a list of possibly useful commands:

.. code:: shell

   ## See the jobs you've submitted, and refresh them every 2 seconds
   watch condor_q --submitter foobar

   ## See if how many machines have GPUs, and how many are available
   condor_status --constraint "CUDADriverVersion>=10.1" -total

   ## See the stats on GPU machines (including GPU name)
   condor_status -compact -constraint 'TotalGpus > 0' -af Machine TotalGpus CUDADeviceName CUDACapability

   ## See how much CUDA memory on each machine (and how many are available)
   condor_status --constraint "CUDADriverVersion>=10.1" -attributes CUDAGlobalMemoryMb -json
   # See which machines have that memory
   # Also write JSON file so readable by Pandas read_json
   condor_status --constraint "CUDADriverVersion>=10.1" -attributes CUDAGlobalMemoryMb -attribute Machine -json >> stats.json

   ## See how many GPUs are available
   condor_status --constraint "CUDADriverVersion>=10.1" -total

``CUDAGlobalMemoryMb`` is not the only attribute that can be displayed; a more
complete list is at
https://htcondor.readthedocs.io/en/latest/classad-attributes/machine-classad-attributes.html.

.. _condor_q: https://htcondor.readthedocs.io/en/latest/man-pages/condor_q.html
.. _condor_status: https://htcondor.readthedocs.io/en/latest/man-pages/condor_status.html
.. _condor_submit: https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html


