# Copyright 2020 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional, Union, Tuple

import os
import shutil
import functools
import pickle

from pathlib import Path
from urllib.parse import urlunsplit

from . import names, utils, exceptions


@functools.total_ordering
class TransferPath:
    """
    A :class:`TransferPath` describes the location of a file or directory.
    If the ``protocol`` and ``location`` are both ``None``, it describes a
    location on the local filesystem. If either are given, it describes
    a remote location.

    When used as an argument to a mapped function, a :class:`TransferPath`
    tells HTMap to arrange for the specified files/directories to be transferred
    to the execute machine from some location, which may be the local filesystem
    on the submit machine or some remote location like an HTTP address or an
    S3 server.

    Transfer paths are recognized in mapped function inputs as long as they are
    either:

    #. Arguments or keyword arguments of the mapped function.
    #. Stored inside a primitive container (tuple, list, set, dictionary value)
       that is an argument or keyword argument of the mapped function. Nested
       containers are inspected recursively.

    When the mapped function runs execute-side, it will receive
    (instead of this object) a normal :class:`pathlib.Path` object pointing to
    the execute-side path of the file/directory.

    :class:`TransferPath` is also used to specify the locations for output files
    to be sent, if they are not to be returned to the submit machine.
    For example, output files could be sent to an S3 server.
    See the ``output_remaps`` argument of :class:`MapOptions` for more details
    on "remapped" output file transfer.

    Where appropriate, :class:`TransferPath` has the same interface as a
    :class:`pathlib.Path`. See the examples for some ways to leverage this API
    to efficiently construct transfer paths.

    .. attention::

        You may need to pass additional submit descriptors to your map
        to actually be able to use input/output transfers for certain protocols.
        For example, to transfer to and from an S3 server, you also need to pass
        ``aws_access_key_id_file`` and ``aws_secret_access_key_file``.
        See the
        `condor_submit documentation <https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html>`_
        for more details.


    Examples
    --------

    Transfer a file stored in your home directory using HTCondor file transfer:

    .. code-block::

        transfer_path = htmap.TransferPath.cwd() / 'file.txt'

    Transfer a local file at an absolute path using HTCondor file transfer:

    .. code-block::

        transfer_path = htmap.TransferPath("/foo/bar/baz.txt")

    Get a file from an HTTP server, located at
    ``http://htmap.readthedocs.io/en/latest/_static/htmap-logo.svg``:

    .. code-block::

        transfer_path = htmap.TransferPath(
            path = "en/latest/_static/htmap-logo.svg",
            protocol = "http",
            location = "htmap.readthedocs.io",
        )

    or

    .. code-block::

        base_path = htmap.TransferPath(
            path = "/",
            protocol = "http",
            location = "htmap.readthedocs.io",
        )
        transfer_path = base_path / 'en' / 'latest' / '_static' / 'htmap-logo.svg'

    """

    def __init__(
        self,
        path: Union["TransferPath", os.PathLike],
        protocol: Optional[str] = None,
        location: Optional[str] = None,
    ):
        """
        Parameters
        ----------
        path
            The path to the file or directory to transfer.
        protocol
            The protocol to perform for the transfer with.
            If set to ``None`` (the default), use HTCondor local file transfer.
        location
            The location to find a remote file when using a protocol transfer.
            This could be the address of a server, for example.
        """
        if isinstance(path, type(self)):
            protocol = protocol or path.protocol
            location = location or path.location
            path = path.path

        # you can't have a location without a protocol
        if location is not None and protocol is None:
            raise ValueError(f"If a {self.__class__.__name__} has a location, it must have a protocol as well.")

        self.path = Path(path)
        self.protocol = protocol
        self.location = location

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.as_url() == other.as_url()

    def __hash__(self):
        return hash((self.__class__, self.as_url()))

    def __le__(self, other):
        return self.as_url() < other.as_url()

    def __repr__(self):
        return f"{self.__class__.__name__}(path={repr(self.path.as_posix())}, protocol={repr(self.protocol)}, location={repr(self.location)})"

    @property
    def _parts(self):
        return (
            self.protocol or "",
            self.location or "",
            self.path.as_posix()
            if self.protocol is not None
            else self.path.absolute().as_posix(),
            "",
            "",
        )

    def as_url(self):
        return urlunsplit(self._parts)

    def __getattr__(self, item):
        # attempt to forward unknown attribute access to our Path
        try:
            x = getattr(self.path, item)
            if isinstance(x, Path):
                return TransferPath(x, protocol = self.protocol, location = self.location)
            elif callable(x):
                return lambda *args, **kwargs: _convert(self, x, *args, **kwargs)
            return x
        except AttributeError as e:
            raise AttributeError(
                f"'{self.__class__.__name__}' has no attribute '{item}'"
            )

    def __truediv__(self, other):
        return self.__class__(
            self.path / other if not isinstance(other, TransferPath) else other.path, protocol = self.protocol, location = self.location
        )

    @classmethod
    def cwd(cls):
        return cls(Path.cwd())

    @classmethod
    def home(cls):
        return cls(Path.home())

    def __getstate__(self):
        return self.path, self.location, self.protocol

    def __setstate__(self, state):
        self.path, self.location, self.protocol = state


def _convert(self, x, *args, **kwargs):
    y = x(*args, **kwargs)
    return TransferPath(y, protocol = self.protocol, location = self.location) if isinstance(y, Path) else y


def transfer_output_files(*paths: os.PathLike) -> None:  # pragma: execute-only
    """
    Informs HTMap about the existence of output files.

    .. attention::

        This function is a no-op when executing locally, so you if you're
        testing your function it won't do anything.

    .. attention::

        The files will be **moved** by this function, so they will not be
        available in their original locations.

    Parameters
    ----------
    paths
        The paths to the output files.
    """
    # no-op if not on execute node
    if os.getenv('HTMAP_ON_EXECUTE') != "1":
        return

    scratch_dir = Path(os.environ['_CONDOR_SCRATCH_DIR'])

    user_transfer_dir = scratch_dir / names.USER_TRANSFER_DIR / os.environ['HTMAP_COMPONENT']
    user_url_transfer_dir = scratch_dir / names.USER_URL_TRANSFER_DIR
    user_transfer_cache = scratch_dir / names.TRANSFER_PLUGIN_CACHE

    destination: Optional[TransferPath]

    for path in paths:
        if isinstance(path, tuple):
            if not utils.CAN_USE_URL_OUTPUT_TRANSFER:
                raise exceptions.InsufficientHTCondorVersion("HTMap URL output transfer requires HTCondor v8.9.2 or later.")
            path, destination = path
        else:
            path, destination = path, None

        path = Path(path).absolute()

        if destination is None:  # condor file transfer
            target = user_transfer_dir / path.relative_to(scratch_dir)
        else:  # url file transfer
            target = user_url_transfer_dir / path.relative_to(scratch_dir)
            h = str(hash((target, destination)))
            user_transfer_cache.mkdir(exist_ok = True)
            with (user_transfer_cache / h).open(mode = 'wb') as f:
                pickle.dump((target, destination.as_url()), f)

        target.parent.mkdir(exist_ok = True, parents = True)
        shutil.move(str(path), str(target))
