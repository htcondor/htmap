HTMap
======

.. py:currentmodule:: htmap

HTMap is a library that wraps the process of mapping Python function calls out to an HTCondor pool.
It provides tools for submitting, managing, and processing the output of arbitrary functions.

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

:doc:`tutorials`
   Tutorials on using HTMap.

:doc:`dependencies`
   Information about how to manage your dependencies.

:doc:`api`
   Public API documentation.

:doc:`settings`
   Documentation for the various settings.

:doc:`recipes`
   Deeper dives on specific, common tasks.

:doc:`tips-and-tricks`
   Useful code snippets, tips, and tricks.

:doc:`faq`
   These questions are asked, sometimes frequently.

:doc:`devs`
   How HTMap works under the hood; how to set up a development environment.


.. toctree::
   :maxdepth: 2
   :hidden:

   self
   tutorials
   dependencies
   api
   settings
   recipes
   tips-and-tricks
   faq
   devs

