import os
import pathlib


class TransferPath(pathlib.Path):
    """
    Identical to :class:`pathlib.Path`, except that it triggers HTMap's
    automatic input file transfer. See the "Working with Files" tutorial for an example.
    """

    _flavour = pathlib._windows_flavour if os.name == 'nt' else pathlib._posix_flavour

    def __new__(cls, *args, **kwargs):
        if cls is TransferPath:
            cls = TransferWindowsPath if os.name == 'nt' else TransferPosixPath

        return super().__new__(cls, *args, **kwargs)


class TransferWindowsPath(TransferPath):
    pass


class TransferPosixPath(TransferPath):
    pass
