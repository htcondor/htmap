.. _cli:

CLI Reference
=============

HTMap provides a command line tool called ``htmap`` that exposes a subset
of functionality focused around monitoring long-running maps without needing
to run Python yourself.

View the available sub-commands by running:

.. code:: shell

   htmap --help  # View available commands

Some useful commands are highlighted in the Tips and Tricks section at
:ref:`cli-tips`.

Here's the full documentation on all of the available commands:

.. click:: htmap.cli:cli
   :prog: htmap
   :show-nested:
