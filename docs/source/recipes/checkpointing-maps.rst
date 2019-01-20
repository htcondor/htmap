.. py:currentmodule:: htmap

Checkpointing Maps
------------------

When running on opportunistic resources, HTCondor might "evict" your map components from the execute locations.
Evicted components return to the queue and, without your intervention, restart from scratch.
However, HTMap can preserve files across an eviction and make them available in the next run.
You can use this to write a function that can resume from partial progress when it restarts.

The important thing for you to think about is that **HTMap will always run your function from the start**.
That means that the general structure of a checkpointing function should look like this:

.. code-block:: python

    def function(inputs):
        try:
            # attempt to reload from a checkpoint file
        except (FileNotFoundError, ...):  # any errors that indicate that the checkpoint doesn't exist, is corrupt, etc.
            # initialize from input data

        # do work

Your work must be written such that it doesn't care where it starts.
Generally that means you'll need to replace ``for`` loops with ``while`` loops.
For example, a simulation that proceeds in 100 steps like this:

.. code-block:: python

    import htmap

    @htmap.mapped
    def function(initial_state):
        current_state = initial_state
        for step in range(100):
            current_state = evolve(current_state)

        return current_state

would need to become something like

.. code-block:: python

    import htmap

    @htmap.mapped
    def function(initial_state):
        try:
            current_step, current_state = load_from_checkpoint(checkpoint_file)
        except FileNotFoundError:
            current_step, current_state = 0, initial_state

        while current_step < 100:
            current_state = evolve(current_state)
            current_step += 1

            if should_write_checkpoint:
                write_checkpoint(current_step, current_state)
                htmap.checkpoint(checkpoint_file)  # important!

        return current_state

Note the call to :func:`htmap.checkpoint`.
This function takes the paths to the checkpoint file(s) that you've written and does the necessary behind-the-scenes handling to make them available if the component needs to restart.
If you don't call this function, the files will not be available, and your checkpoint won't work!

Concrete Example
================

Let's work with a more concrete example.
Here's the function, along with some code to run it and prove that it checkpointed:

.. code-block:: python

    from pathlib import Path
    import time

    import htmap


    @htmap.mapped
    def counter(num_steps):
        checkpoint_path = Path('checkpoint')
        try:
            step = int(checkpoint_path.read_text())
            print('loaded checkpoint!')
        except FileNotFoundError:
            step = 0
            print('starting from scratch')

        while True:
            time.sleep(1)
            step += 1
            print(f'completed step {step}')

            if step >= num_steps:
                break

            checkpoint_path.write_text(str(step))
            htmap.checkpoint(checkpoint_path)

        return True


    map = counter.map('chk', [30])

    while map.component_statuses[0] is not htmap.ComponentStatus.RUNNING:
        print(map.component_statuses[0])
        time.sleep(1)

    print('component has started, letting it run...')
    time.sleep(10)
    map.vacate()
    print('vacated map')

    while map.component_statuses[0] is not htmap.ComponentStatus.COMPLETED:
        print(map.component_statuses[0])
        time.sleep(1)

    print(map[0])
    print(map.stdout(0))


The function itself just sleeps for the given amount of time, but it does it in incremental steps so that we can checkpoint its progress.
We write checkpoints to a file named ``checkpoint`` in the current working directory of the script when it executes.
We try to load the current step number (stored as text, so we need to convert it to an integer) from that file when we start, and if that fails we start from the beginning.
We write a checkpoint after each step, which is overkill (see the next section), but easy to implement for this short example.

The rest of the code (after the function definition) is just there to prove that the example works.
If we run this script, we should see something like this:

.. code-block:: none

    IDLE
    # many IDLE messages
    IDLE
    component has started, letting it run...
    vacated map
    RUNNING
    IDLE
    # more IDLE messages
    IDLE
    RUNNING
    # many RUNNING messages
    RUNNING
    True  # this is map[0]: it's True, not None, so the function finished successfully

    # a bunch of debug information from the stdout of the component

    ----- MAP COMPONENT OUTPUT START -----

    loaded checkpoint!  # we did it!
    completed step 10
    completed step 11
    completed step 12
    completed step 13
    completed step 14
    completed step 15
    completed step 16
    completed step 17
    completed step 18
    completed step 19
    completed step 20
    completed step 21
    completed step 22
    completed step 23
    completed step 24
    completed step 25
    completed step 26
    completed step 27
    completed step 28
    completed step 29
    completed step 30

    -----  MAP COMPONENT OUTPUT END  -----

    Finished executing component at 2019-01-20 08:34:31.130818

We successfully started from step 10!
For a long-running computation, this could represent a significant amount of work.
Long-running components on opportunistic resources might be evicted several times during their life, and without checkpointing, may never finish.

Checkpointing Strategy
======================

You generally don't need to write checkpoints very often.
We recommend writing a new checkpoint if a certain amount of time has elapsed, perhaps an hour.
For example, using the ``datetime`` library:

.. code-block:: python

    import datetime

    import htmap

    @htmap.mapped
    def function(inputs):
        latest_checkpoint_at = datetime.datetime.now()

        # load for checkpoint or initialize

        while not_done:
            # do a unit of work

            if datedate.datetime.now() > latest_checkpoint_at + datetime.timedelta(hours = 1):
                # write checkpoint
                latest_checkpoint_at = datetime.datetime.now()

        return result


Checkpointing Caveats
=====================

Checkpointing does introduce some complications with HTMap's metadata tracking system.
In particular, HTMap only tracks the runtime, stdout, and stderr of the **last execution** of each component.
If your components are vacated and start again from a checkpoint, you'll only see the execution time, standard output, and standard error from the second run.
If you need that information, you should track it yourself inside your checkpoint files.
