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

    for step in range(100):
        next_state = evolve(current_state)

would need to become something like

.. code-block:: python

    try:
        current_step, current_state = load_from_checkpoint(checkpoint_file)
    except FileNotFoundError:
        current_step, current_state = 0, initial_state

    while current_step < 100:
        next_state = evolve(current_state)

        if should_write_checkpoint:
            write_checkpoint(current_step, current_state)


Concrete Example
================


