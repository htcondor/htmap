from typing import Dict, Any, Union, Tuple, Mapping

import os

from htmap import transfer

KWARGS = Dict[str, Any]
ARGS = Tuple[Any, ...]
ARGS_OR_KWARGS = Union[ARGS, KWARGS]
ARGS_AND_KWARGS = Tuple[ARGS, KWARGS]
ITEMDATUM = Dict[str, str]
TRANSFER_PATH = Union[os.PathLike, transfer.TransferPath]
REMAPS = Mapping[str, transfer.TransferPath]
