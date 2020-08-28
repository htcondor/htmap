How to Release a New HTMap Version
==================================

.. py:currentmodule:: htmap

To release a new version of HTMap:

#. Run ``binder/exec.sh``, check that they executed correctly by loading them
   up in a Jupyter session, and commit the resulting executed tutorial notebooks
   into the repository.
#. Make sure that the version PR actually bumps the version in ``setup.cfg``.
#. Merge the version PR into ``master`` via GitHub.
#. Make a GitHub release from https://github.com/htcondor/htmap/releases,
   based on `master`.
   Name it exactly ``vX.Y.Z``, and link to the release notes for that version
   (like https://htmap.readthedocs.io/en/latest/versions/vX_Y_Z.html )
   in the description (the page will not actually exist yet).
#. Delete anything in the ``dist/`` directory in your copy of the repository.
#. On your machine, make sure ``master`` is up-to-date, then run
   ``python3 setup.py sdist bdist_wheel`` to create the source distribution
   and the wheel.
#. Install Twine: ``pip install twine``.
#. Upload to PyPI:
   ``python3 -m twine upload dist/*``.
   You will be prompted for your PyPI login.
