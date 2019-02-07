import os
import pathlib


class TransferPath(pathlib.Path):
    _flavour = pathlib._windows_flavour if os.name == 'nt' else pathlib._posix_flavour

    def __new__(cls, *args, **kwargs):
        if cls is TransferPath:
            cls = TransferWindowsPath if os.name == 'nt' else TransferPosixPath

        return super().__new__(cls, *args, **kwargs)


class TransferWindowsPath(TransferPath):
    pass


class TransferPosixPath(TransferPath):
    pass
