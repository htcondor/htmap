HTMap
======

.. py:currentmodule:: htmap

HTMap is a library that wraps the process of mapping Python function calls out to an `HTCondor pool`_.
It provides tools for submitting, managing, and processing the output of arbitrary functions.

.. _HTCondor pool: https://htcondor.readthedocs.io/

Our goal is to provide as transparent an interface as possible to
high-throughput computing resources so that you can spend more time thinking about your own code,
and less about how to get it running on a cluster.

Running a map over a Python function is as easy as

.. code-block:: python

    import htmap

    def double(x):
        return 2 * x

    doubled = list(htmap.map(double, range(10)))
    print(doubled)
    # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]


If you're just getting started, jump into the first tutorial: :doc:`tutorials/first-steps`.

Happy mapping!

:doc:`installation`
   Installing HTMap

.. note::

    Bug reports and feature requests should go on our `GitHub issue tracker <https://github.com/htcondor/htmap/issues>`_.

:doc:`tutorials`
   Tutorials on using HTMap.

:doc:`dependencies`
   Information about how to manage your what your code depends on (e.g., other Python packages).

:doc:`api`
   Public API documentation.

:doc:`cli`
   Use of the HTMap CLI.

:doc:`htcondor`
   Tips on using HTMap with HTCondor

:doc:`tips-and-tricks`
   Useful tips & tricks on the API.

:doc:`faq`
   These questions are asked, sometimes frequently.

:doc:`settings`
   Documentation for the various settings.

:doc:`version-history`
   New features, bug fixes, and known issues by version.

:doc:`devs`
   How to contribute to HTMap, how to set up a development environment, how HTMap works under the hood, etc.

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Getting started

   installation
   tutorials
   htcondor
   tips-and-tricks
   faq

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Use and Reference

   dependencies
   api
   cli
   settings

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Develop

   version-history
   devs

