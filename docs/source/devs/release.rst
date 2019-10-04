How to Release a New HTMap Version
==================================

.. py:currentmodule:: htmap


To release a new version of HTMap:

1. Merge the version PR into ``master`` via GitHub.
1. Make a GitHub release from https://github.com/htcondor/htmap/releases .
Name it exactly ``vX.Y.Z``, and link to the release notes for that version
(like https://htmap.readthedocs.io/en/latest/versions/vX_Y_Z.html )
in the description (the page will not actually exist yet).
1. Delete anything in the ``dist/`` directory in your copy of the repository.
1. On your machine, make sure ``master`` is up-to-date, then run
``python3 setup.py sdist bdist_wheel`` to create the source distribution
and the wheel. (This is where the files in ``dist/`` are created.)
1. Install Twine: ``pip install twine``.
1. Upload to PyPI:
``python3 -m twine upload dist/*``.
You will be prompted for your PyPI login.
