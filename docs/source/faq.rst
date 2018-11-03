FAQ
===

.. py:currentmodule:: htmap

.. _install:

How do I install HTMap?
-----------------------

.. caution::

    The instructions given below will eventually be true, but we're not on PyPI yet.
    For the moment you should install HTMap by running ``pip install git+https://github.com/htcondor/htmap.git``.

On Unix/Linux systems, ``pip install htmap`` should suffice.

* To get the latest development version of HTMap, run ``pip install git+https://github.com/htcondor/htmap.git`` instead.
* Run ``pip install git+https://github.com/htcondor/htmap.git@<branch>`` to install a specific branch.
* You may need to append ``--user`` to the ``pip`` command if you do not have permission to install packages directly into the Python you are using.

On Windows, append ``--no-dependencies`` to the ``pip`` command.
Currently, the HTCondor bindings package cannot be installed on Windows via ``pip``.
The bindings themselves are put on your Python path when you install HTCondor on your system.
You'll need to install HTMap's other dependencies manually - see the ``requirements.txt`` file in the repository root.

I'm getting a weird error from ``cloudpickle.load``?
----------------------------------------------------

You probably have a version mismatch between the submit and execute locations.
See the "Attention" box near the top of :doc:`dependencies`.


What's a good minimal custom Docker image for using HTMap?
----------------------------------------------------------

I'm glad you asked!
Here's the Dockerfile template that Josh uses:

.. code-block:: docker

    FROM continuumio/miniconda3

    # update & upgrade
    RUN apt-get -y update && \
        apt-get -y upgrade

    # install python dependencies
    RUN conda install -y python=<your preferred python version> \
                         cloudpickle \
        && \
        conda update -y --all && \
        conda clean -y --all

    # my custom packages
    RUN <conda/pip> install <your dependencies, however you'd like>

