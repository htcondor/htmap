Using HTCondor with HTMap
=========================

HTMap is only a Python wrapper to the HTCondor API. That means the vast
majority of the HTCondor functionality is available.  Here's a brief overview
of how to use HTCondor with HTMap:

Component and Job States
-------------------------------------
Each HTMap map component is represented by an HTCondor job.
Map components will usually be in one of four states:

* **Idle**: the job/component has not started running yet; it is waiting to be assigned resources to execute on.
* **Running**: the job/component is running on an execute machine.
* **Held**: HTCondor has decided that it can't run the job/component, but that you (the user) might be able to fix the problem. The job will try to run again if it released.
* **Completed**: the job/component has finished running, and HTMap has collected its output.

For more detail, see the relevant HTCondor documentation:

* https://htcondor.readthedocs.io/en/latest/users-manual/managing-a-job.html#checking-on-the-progress-of-jobs
* https://htcondor.readthedocs.io/en/latest/admin-manual/policy-configuration.html#machine-states

Requesting commonly used resources
----------------------------------

HTCondor's default configuration can be limiting -- what if your job requires
more memory or more disk space? HTCondor jobs can request resources, and
HTMap supports those requests via :class:`~htmap.MapOptions`.

:class:`~htmap.MapOptions` accepts many of the same keys that `condor_submit`_
accepts.  Some of the more commonly requested keys are:

* ``request_memory``. Possible values are like ``"1MB`` for 1MB, or ``"2GB"`` for 2GB of
  memory.
* ``request_cpus``. Possible values are like ``"1"`` for 1 CPU, or ``"2"`` for 2 CPUs.
* ``request_disk`` to request an amount of disk space. Possible values are like ``"10GB"`` for 10GB, or ``"1TB"`` for 1 terabyte.
  
If any of the resource requests are not set, the default values set by your HTCondor cluster administrator will be used.

These would be set with :class:`~htmap.MapOptions`. For example, this code
might be used:

.. code:: python

   options = htmap.MapOptions(
       request_cpus="1",
       request_disk="10GB",
       request_memory="4GB",
   )
   htmap.map(..., map_options=options)

When it's mentioned that "the option ``foo`` needs to be set" (possibly in a
submit file), this corresponds to adding the option in the appropriate place in
:class:`~htmap.MapOptions`.

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

HTMap tries to expose a complete interface for submitting and managing jobs,
but not for examining the state of your HTCondor pool itself.
Here are some HTCondor shell commands that you may find useful:

* `condor_q`_: seeing the jobs submitted to the scheduler (aliased to
  :func:`htmap.status`)
* `condor_status`_: seeing resources the different machines have

The links go an HTML version of the man pages; their also visible with ``man``
(e.g., ``man condor_q``).  Here's a list of possibly useful commands:

.. code:: shell

   ## See the jobs user foobar has submitted, and their status
   condor_q --submitter foobar

   ## See if how many machines have GPUs, and how many are available
   condor_status --constraint "CUDADriverVersion>=10.1" -total

   ## See the stats on GPU machines (including GPU name)
   condor_status -compact -constraint 'TotalGpus > 0' -af Machine TotalGpus CUDADeviceName CUDACapability

   ## See how much CUDA memory on each machine (and how many are available)
   condor_status --constraint "CUDADriverVersion>=10.1" -attributes CUDAGlobalMemoryMb -json
   # See which machines have that much memory
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
