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

