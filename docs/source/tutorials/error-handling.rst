.. _tutorial-error-handling:

Error Handling
==============

.. py:currentmodule:: htmap

Sometimes your map components will fail out on the pool.
This is annoying because you don't have access to the environment where the component is executing, so it can be difficult to debug what's going on.
HTMap provides some tools to help you debug these kinds of errors.

Let's define a map that will have a failing component:

.. code-block:: python

    import htmap

    def inverse(x):
        return 1 / x

    result = htmap.map('inv', inverse, range(10))

This map hides a problem: the first input is ``0``, but evaluating ``1 / 0`` raises an exception:

.. code-block:: python

    1 / 0

    # Traceback (most recent call last):
    #   File "<input>", line 1, in <module>
    # ZeroDivisionError: division by zero

The simplest kinds of problems are that your components might be "held" by HTCondor for some reason.
If we wait a little while and check the status of our jobs, we'll see that one of them is held:

.. code-block:: python

    print(result.status())

    # Map inv (10 inputs): Held = 1 | Idle = 0 | Run = 0 | Done = 9


To find out why, use :class:`htmap.Map.hold_reasons()`:

.. code-block:: python

    print(result.hold_reasons()

    #  Input Index │ Hold Reason Code │                                                                                                                                         Hold Reason
    # ─────────────┼──────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    #       0      │        13        │ Error from slot1@thyrllan: STARTER at xx.xxx.xxx.xxx failed to send file(s) to <xx.xxx.xxx.xxx:xxxx>: error reading from \condor\execute\dir_4436\d238998870ae18a399d03477dad0c0a8.out: (errno 2) No such file or directory; SHADOW failed to receive file(s) from <xx.xxx.xxx.xxx:xxxx>
    # ─────────────┴──────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────


This doesn't mean much because it has to do with what HTCondor knows about our map, which is just that it expected a certain file to exist and it doesn't.
The long filename there is the output file that HTMap is using to communicate results back, so the error just means that we didn't produce an output file.
What **is** important is that this tells us in the index of the input that got held: ``0``.

To determine what caused the output file to not exist for input ``0`` , we can look at the standard output and standard error streams from that map component while it was executing:

.. code-block:: python

    print(result.output(0))

    # Landed on execute node xxx.yyy.zzz.www (xxx.yyy.zzz.www) at 2018-10-02 16:00:20.434779
    # Local directory contents:
    #     condor\execute\dir_4436\.chirp.config
    #     condor\execute\dir_4436\.job.ad
    #     condor\execute\dir_4436\.machine.ad
    #     condor\execute\dir_4436\condor_exec.py
    #     condor\execute\dir_4436\d238998870ae18a399d03477dad0c0a8.in
    #     condor\execute\dir_4436\func
    #     condor\execute\dir_4436\_condor_stderr
    #     condor\execute\dir_4436\_condor_stdout
    # Running
    #     <function inverse at 0x000001C9CEDA41E0>
    # with args
    #     (0,)
    # and kwargs
    #     {}
    # from input hash
    #     d238998870ae18a399d03477dad0c0a8


This is just information printed by HTMap when our component starts executing.
If we printed or logged anything ourselves, we would see it here as well (but we didn't).
Nothing here is surprising, so we also look at the standard error, which is where exception tracebacks will show up:

.. code-block:: python

    print(result.error(0))

    # Traceback (most recent call last):
    #   File "C:\condor\execute\dir_4436\condor_exec.py", line 83, in <module>
    #     main(arg_hash = sys.argv[1])
    #   File "C:\condor\execute\dir_4436\condor_exec.py", line 77, in main
    #     output = func(*args, **kwargs)
    #   File "<input>", line 2, in inverse
    # ZeroDivisionError: division by zero


Aha!
Now we know that when the input to our function is zero, we get a :class:`ZeroDivisionError`.
Some local debugging will quickly reveal that the problem is that we're trying to do ``1 / 0``.
