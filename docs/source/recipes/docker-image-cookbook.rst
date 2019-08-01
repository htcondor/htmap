.. py:currentmodule:: htmap

Docker Image Cookbook
=====================

Docker is, essentially, a way to send a self-contained computer called a container to another person.
You define the software that goes into the container,
and then anyone with Docker installed on their own computer (the "host")
can run your container and access the software inside
without that sofware being installed on the host.
This is an enormous advantage in distributed computing,
where it can be difficult to ensure that software that your own software depends on ("dependencies")
are installed on the computers your code actually runs on.

To use Docker, you write a **Dockerfile** which tells Docker how to generate an **image**,
which is a blueprint to construct a **container**.
The Dockerfile is a list of instructions, such as shell commands or instructions
for Docker to copy files from the build environment into the image.
You then tell Docker to "build" the image from the Dockerfile.

For use with HTMap, you then upload this image to `Docker Hub <https://hub.docker.com>`_,
where it can then be downloaded to execute nodes in an HTCondor pool.
When your HTMap component lands on an execute node, HTCondor will download your
image from Docker Hub and run your code inside it using HTMap.

The following sections describe, roughly in order of increasing complexity,
different ways to build Docker images for use with HTMap.
Each level of complexity is introduced to solve a more advanced dependency management problem.
We recommend reading them in order until reach one that works for your dependencies
(each section assumes knowledge of the previous sections).

More detailed information on how Dockerfiles work can be found
`in the Docker documentation itself <https://docs.docker.com/engine/reference/builder/>`_
This page only covers the bare minimum to get started with HTMap and Docker.

.. attention::

    This recipe only covers using Docker for **execute-side** dependency management.
    You still need to install dependencies **submit-side** to launch your map in the first place!


Can I use HTMap's default image?
--------------------------------

HTMap's default Docker image is `htcondor/htmap-exec <https://hub.docker.com/r/htcondor/htmap-exec/>`_,
which is itself based on`continuumio/anaconda3 <https://hub.docker.com/r/continuumio/anaconda3/>`_.
It is based on Python 3 and has many useful packages pre-installed, such as ``numpy``, ``scipy``, and ``pandas``.
If your software only depends on packages included in the `Anaconda distribution <https://docs.anaconda.com/anaconda/packages/pkg-docs/>`_,
you can use HTMap's default image and won't need to create your own.


I depend on Python packages that aren't in the Anaconda distribution
--------------------------------------------------------------------

.. attention::

    Before proceeding, `install Docker on your computer <https://docs.docker.com/install/#supported-platforms>`_
    and `make an account on Docker Hub <https://hub.docker.com/>`_.


Let's pretend that there's a package called ``foobar`` that your Python function depends on,
but isn't part of the Anaconda distribution.
You will need to write your own Dockerfile to include this package in your Docker image.

Docker images are built in **layers**.
You always start a Dockerfile by stating which existing Docker image you'd like to use as your base layer.
A good choice is the same Anaconda image that HTMap uses as the default,
which comes with both the ``conda`` package manager and the standard ``pip``.
Create a file named ``Dockerfile`` and write this into it:

.. code-block:: docker

    # Dockerfile

    FROM continuumio/anaconda3:latest

    RUN pip install --no-cache-dir htmap

    ARG USER=htmap
    RUN groupadd ${USER} \
     && useradd -m -g ${USER} ${USER}
    USER ${USER}

Each line in the Dockerfile starts with a short, capitalized word which tells Docker what kind of build instruction it is.

* ``FROM`` means "start with this base image".
* ``RUN`` means "execute these shell commands in the container".
* ``ARG`` means "set build argument" - it acts like an environment variable that's only set during the image build.

Lines that begin with a ``#`` are comments in a Dockerfile.
The above lines say that we want to inherit from the image ``continuumio/anaconda3:latest`` and build on top of it.
To be compatible with HTMap, we install ``htmap`` via ``pip``.
We also set up a non-root user to do the execution, which is important for security.
Naming that user ``htmap`` is arbitrary and has nothing to do with the ``htmap`` package itself.

Now we need to tell Docker to run a shell command during the build to install ``foobar``
by adding one more line to the bottom of the Dockerfile.

.. code-block:: docker

    # Dockerfile

    FROM continuumio/anaconda3:latest

    RUN pip install --no-cache-dir htmap

    ARG USER=htmap
    RUN groupadd ${USER} \
     && useradd -m -g ${USER} ${USER}
    USER ${USER}

    # if foobar can be install via conda, use these lines
    RUN conda install -y foobar \
     && conda clean -y --all

    # if foobar can be installed via pip, use these lines
    RUN pip install --no-cache-dir foobar

Some notes on the above:

* If you need to install some packages via ``conda`` and some via ``pip``, you may need to use both types of lines.
* The ``conda clean`` and ``--no-cache-dir`` instructions for ``conda`` and ``pip`` respectively just help keep the final Docker image as small as possible.
* The ``-y`` options for the ``conda`` commands are the equivalent of answering "yes" to questions that ``conda`` asks on the command line, since the Docker build is non-interactive.
* A trailing ``\`` is a line continuation, so that first command is equivalent to running ``conda install -y foobar && conda clean -y --all``, which is just ``bash`` shorthand for "do both of these things".

If you need install many packages, we recommend writing a ``requirements.txt`` file (see `the docs <https://pip.pypa.io/en/stable/user_guide/#requirements-files>`_) and using

.. code-block:: docker

    # Dockerfile

    FROM continuumio/anaconda3:latest

    RUN pip install --no-cache-dir htmap

    ARG USER=htmap
    RUN groupadd ${USER} \
     && useradd -m -g ${USER} ${USER}
    USER ${USER}

    COPY requirements.txt requirements.txt
    RUN pip install --no-cache-dir -r requirements.txt

The ``COPY`` build instruction tells Docker to copy the file ``requirements.txt`` (path relative to the build directory, explained below)
to the path ``requirements.txt`` inside the image.
Relative paths inside the container work the same way they do in the shell; the image has a "working directory" that you can set using the ``WORKDIR`` instruction.

Now that we have a Dockerfile, we can tell Docker to use it to build an image.
You'll need to choose a descriptive name for the image, ideally something easy to type that's related to your project (like ``qubits`` or ``gene-analysis``).
Wherever you see ``<image>`` below, insert that name.
You'll also want to version your images by adding a "tag" after a ``:``, like ``<image>:v1``, ``<image>:v2``, ``<image>:v3``, etc.
You can use any string you'd like for the tag.
You'll also need to know your Docker Hub username.
Wherever you see ``<username>`` below, insert your username, and wherever you see ``<tag>``, insert your chosen version tag.

At the command line, in the directory that contains ``Dockerfile``, run

.. code-block:: bash

    $ docker build -t <username>/<image>:<tag> .

You should see the output of the build process, hopefully ending with

.. code-block:: bash

    Successfully tagged <username>/<image>:<tag>

``<username>/<image>:<tag>`` is the universal identifier for your image.

Now we need to push the image up to Docker Hub.
Run

.. code-block:: bash

    $ docker push <username>/<image>:<tag>

You'll be asked for your credentials, and then all of the data for your image will be pushed up to Docker Hub.
Once this is done, you should be able to use the image with HTMap.
Change your HTMap settings (see :ref:`settings-docker`) to point to your new image, and launch your maps!


I don't need most of the Anaconda distribution and want to use a lighter-weight base image
------------------------------------------------------------------------------------------

Instead of using the full Anaconda distribution, use a base Docker image that only includes the ``conda`` package manager:

.. code-block:: docker

    # Dockerfile

    FROM continuumio/miniconda3:latest

    RUN pip install --no-cache-dir htmap

    ARG USER=htmap
    RUN groupadd ${USER} \
     && useradd -m -g ${USER} ${USER}
    USER ${USER}

From here, install your particular dependencies as above.

If you prefer to not use ``conda``, an even-barer-bones image could be produced from

.. code-block:: docker

    # Dockerfile

    FROM python:latest

    RUN pip install --no-cache-dir htmap

    ARG USER=htmap
    RUN groupadd ${USER} \
     && useradd -m -g ${USER} ${USER}
    USER ${USER}

We use ``python:latest`` as our base image, so we don't have ``conda`` anymore.

I want to use a Python package that's not on PyPI or Anaconda
-------------------------------------------------------------

Perhaps you've written a package yourself, or you want to use a package that is only available as source code on GitHub or a similar website.
There are multiple way to approach this, most of them roughly equivalent.
The first step for all of them is to write a ``setup.py`` file for your package.
Some instructions for writing a ``setup.py`` can be found `here <https://the-hitchhikers-guide-to-packaging.readthedocs.io/en/latest/creation.html#arranging-your-file-and-directory-structure>`_.

Once you have a working ``setup.py``, there are various ways to proceed, in reverse order of complexity:

* Upload your package to PyPI and ``pip install <package>`` as in previous sections.
  This is the least flexible because you'll need to upload to PyPI every time your update your package.
  If you don't own the package, you shouldn't do this!
* Upload your package to a publicly-accessible version control repository and use `pip`'s `VCS support <https://pip.pypa.io/en/stable/reference/pip_install/#vcs-support>`_ to install it
  (for example, if your package is on GitHub, something like ``pip install git+https://github.com/<UserName>/<package>.git``).
* Use the ``COPY`` build instruction to copy your package directly into the Docker image,
  then ``pip install <path/to/dir/containing/setup.py>`` as a ``RUN`` instruction.
  Note that your package will need to be in the Docker build context (see `the docs <https://docs.docker.com/engine/reference/commandline/build/>`_ for details).


I want to use a base image that doesn't come with Python pre-installed
----------------------------------------------------------------------

Say you have an existing Docker image that you need to use (maybe it includes non-Python dependencies that you aren't sure how to install yourself).
You need to add Python to this image so that you can run your own code in it.
We recommend adding ``miniconda`` to the image by adding these lines to your Dockerfile:

.. code-block:: docker

    # Dockerfile

    # see discussion below
    FROM ubuntu:latest
    RUN apt-get -y update \
     && apt-get install -y wget

    # Docker build arguments
    # use the Python version you need
    # default to latest version of miniconda (which can then install any version of Python)
    ARG PYTHON_VERSION=3.6
    ARG MINICONDA_VERSION=latest

    # set install location, and add the Python in that location to the PATH
    ENV CONDA_DIR=/opt/conda
    ENV PATH=${CONDA_DIR}/bin:${PATH}

    # install miniconda and Python version specified in config
    # (and ipython, which is nice for debugging inside the container)
    RUN cd /tmp \
     && wget --quiet https://repo.continuum.io/miniconda/Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh \
     && bash Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh -f -b -p $CONDA_DIR \
     && rm Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh \
     && conda install python=${PYTHON_VERSION} \
     && conda clean -y -all

After this, you can install HTMap and any other Python packages you need as in the preceeding sections.

Note that in this example we based the image on Ubuntu's base image and installed ``wget``,
which we used to download the ``miniconda`` installer.
Depending on your base image, you may need to use a different package manager
(for example, ``yum``) or different command-line file download tool (for example, ``curl``).


.. _build-osg-image:

I want to build an image for use on the Open Science Grid
---------------------------------------------------------

First, read through `OSG's Singularity documentation <https://support.opensciencegrid.org/support/solutions/articles/12000024676-docker-and-singularity-containers>`_.

Based on that, our goal will be to build a Docker image and have OSG convert
it to a Singularity image that can be served by OSG.
The tricky part of this is that Docker's ``ENV`` instruction won't carry over to
Singularity, which is the usual method of etting ``python3`` on the ``PATH``
inside the container.
To remedy this, we will create a special directory structure that Singularity
recognizes and uses to execute instructions with specified environments.

This is not a Singularity tutorial, so the simplest thing to do is copy the entire
`singularity.d` directory that `htmap-exec` uses: https://github.com/htcondor/htmap/tree/master/htmap-exec/singularity.d

Anything you need to specify for your environment should be done in
``singularity.d/env/90-environment.sh``.
This file will be "sourced" (run) when the image starts, before HTMap executes.

In your Dockerfile, you must copy this directory to the correct location inside
the image:

.. code-block:: docker

    # Dockerfile snippet

    COPY <path/to/singularity.d> /.singularity.d


Note the path on the right: a hidden directory at the root of the filesystem.
This is just a Singularity convention.
The left path is just the location of the ``singularity.d`` directory you made.

Note that if you ``FROM`` an ``htmap-exec`` image, this setup will already be embedded
in the image for you.
