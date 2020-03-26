HTCondor tips
=============

What does it mean if a job is held or idle? What exactly does an "errored" or
"completed" job entail? Details are at
https://htcondor.readthedocs.io/en/latest/admin-manual/policy-configuration.html#machine-states

Requesting commonly used resources
----------------------------------

HTCondor's default configuration can be limiting -- what if your job requires
more memory or multiple CPUs?

HTCondor can be configured to allow this, and HTMap supports the
configuration via :func:`~htmap.MapOptions`.

`condor_submit`_ accepts many of the same keys that can go into
:func:`~htmap.MapOptions`. Some of the more commonly requested keys are:

* ``request_memory``. Possible values are ``"1M`` for 1MB, ``"2GB"`` for 2GB of
  memory. If not specified, the HTCondor's defaults are accepted provided
  ``JOB_DEFAULT_REQUESTMEMORY`` is not set (one of the
  `configuration variables`_).
* ``request_cpus``. Possible values are ``"1"`` for 1 CPU.
* ``request_disk``. Possible values are ``"10GB"`` for 10GB, ``"1T"`` for 1
  terabytes.

.. _configuration variables: https://htcondor.readthedocs.io/en/latest/admin-manual/configuration-macros.html

GPUs
----

CHTC has some good guides on this:

* "`Jobs that use GPUs`_"
* "`Run Machine Learning Jobs on the HTC system`_"

.. _Jobs that use GPUs: http://chtc.cs.wisc.edu/gpu-jobs.shtml
.. _Run Machine Learning Jobs on the HTC system: http://chtc.cs.wisc.edu/gpu-jobs.shtml

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

   ## See how much CUDA memory on each machine (and how many are available)
   condor_status --constraint "CUDADriverVersion>=10.1" -attributes CUDAGlobalMemoryMb -json
   # See which machines have that memory
   # Also write JSON file so readable by Pandas read_json
   condor_status --constraint "CUDADriverVersion>=10.1" -attributes CUDAGlobalMemoryMb -attribute Machine -json >> stats.json

``CUDAGlobalMemoryMb`` is not the only attribute that can be displayed; a more
complete list is at
https://htcondor.readthedocs.io/en/latest/classad-attributes/machine-classad-attributes.html.

.. _condor_q: https://htcondor.readthedocs.io/en/latest/man-pages/condor_q.html
.. _condor_status: https://htcondor.readthedocs.io/en/latest/man-pages/condor_status.html
.. _condor_submit: https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html


