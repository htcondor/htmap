from typing import Tuple, Iterable, Dict, Union, Optional, List, Callable, Iterator, Any

import shutil
from pathlib import Path
import itertools
import json

from . import mapping, options, result, exceptions


class MapBuilder:
    def __init__(self, mapper: 'HTMapper', map_id: str, force_overwrite: bool = False):
        self.mapper = mapper
        self.map_id = map_id
        self.force_overwrite = force_overwrite

        self.args = []
        self.kwargs = []

        self._result = None

    def __repr__(self):
        return f'<{self.__class__.__name__}(mapper = {self.mapper})>'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # if an exception is raised in the with, re-raise without submitting jobs
        if exc_type is not None:
            return False

        self._result = self.mapper.starmap(
            self.map_id,
            self.args,
            self.kwargs,
            force_overwrite = self.force_overwrite
        )

    def __call__(self, *args, **kwargs):
        self.args.append(args)
        self.kwargs.append(kwargs)

    @property
    def result(self) -> result.MapResult:
        """
        The :class:`MapResult` associated with this :class:`MapBuilder`.
        Will raise :class:`htmap.exceptions.NoResultYet` when accessed until the ``with`` block for this :class:`MapBuilder` completes.
        """
        if self._result is None:
            raise exceptions.NoResultYet('result does not exist until after with block')
        return self._result

    def __len__(self):
        return len(self.args)


class HTMapper:
    def __init__(
        self,
        func: Callable,
        map_options: Optional[options.MapOptions] = None,
    ):
        self.func = func

        if map_options is None:
            map_options = options.MapOptions()
        self.map_options = map_options

    def __repr__(self):
        return f'<{self.__class__.__name__}(func = {self.func}, map_options = {self.map_options})>'

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def map(
        self,
        map_id: str,
        args: Iterable[Any],
        force_overwrite: bool = False,
        map_options = None,
        **kwargs,
    ) -> result.MapResult:
        return mapping.map(
            map_id,
            self.func,
            args,
            map_options = map_options,
            force_overwrite = force_overwrite,
            **kwargs,
        )

    def starmap(
        self,
        map_id: str,
        args: Optional[Iterable[Tuple]] = None,
        kwargs: Optional[Iterable[Dict]] = None,
        force_overwrite: bool = False,
        map_options = None,
    ) -> result.MapResult:
        return mapping.starmap(
            map_id,
            self.func,
            args,
            kwargs,
            force_overwrite = force_overwrite,
            map_options = map_options,
        )

    def build_map(self, map_id: str, force_overwrite: bool = False):
        """
        Return a :class:`htmap.MapBuilder` for the wrapped function.

        Parameters
        ----------
        map_id
            The ``map_id`` to assign to this map.
        force_overwrite
            If ``True``, and there is already a map with the given ``map_id``, it will be removed before running this one.

        Returns
        -------
        map_builder :
            A :class:`htmap.MapBuilder` for the wrapped function.
        """
        return MapBuilder(mapper = self, map_id = map_id, force_overwrite = force_overwrite)


def htmap(map_options: Optional[options.MapOptions] = None) -> Union[Callable, HTMapper]:
    """
    A decorator that wraps a function in an :class:`HTMapper`,
    which provides an interface for mapping functions calls out to an HTCondor cluster.

    Parameters
    ----------

    Returns
    -------
    mapper
        An :class:`HTMapper` that wraps the function (or a wrapper function that does the wrapping).
    """

    def wrapper(func: Callable) -> HTMapper:
        # prevent nesting HTMappers inside each other by accident
        if isinstance(func, HTMapper):
            func = func.func

        return HTMapper(func, map_options)

    # if called without parens, map_options is actually func!
    if callable(map_options):
        return wrapper(map_options)
    return wrapper
