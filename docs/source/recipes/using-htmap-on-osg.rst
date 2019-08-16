.. py:currentmodule:: htmap

Using HTMap on the Open Science Grid
------------------------------------

Running HTMap with the `Open Science Grid <https://opensciencegrid.org/>`_ (OSG)
requires some special configuration.
The OSG does not support Docker, and is also not amenable to HTMap's own
Singularity delivery mechanism.
However, the OSG does still allow you to run your code inside a Singularity
container.
The ``.htmaprc`` file snippet below sets up HTMap to use this support.

.. code:: toml

    # .htmaprc

    DELIVERY_METHOD = "assume"

    [MAP_OPTIONS]
    requirements = "HAS_SINGULARITY == TRUE"
    "+ProjectName" = "\"<your project name>\""
    "+SingularityImage" = "\"/cvmfs/singularity.opensciencegrid.org/<repo/tag:version>\""


The extra ``"`` on the left are to escape the ``+``, which is not normally legal syntax,
and the extra ``\"`` on the right are to ensure that the actual value is a string.

Note the two places inside ``< >``, where you must supply some information
You must specify your OSG project name, and you must specify which OSG-supplied
Singularity image to use.
For more information on what images are available, see the
`OSG Singularity documentation <https://support.opensciencegrid.org/support/solutions/articles/12000024676-docker-and-singularity-containers>`_.
HTMap's own default image, ``htmap-exec``, is always available on the OSG.
For example, to use ``htmap-exec:v0.4.3``, you would set

.. code:: toml

    "+SingularityImage" = "\"/cvmfs/singularity.opensciencegrid.org/htcondor/htmap-exec:v0.4.3\""


For advice on building your own image for the OSG, see :ref:`build-osg-image`.
