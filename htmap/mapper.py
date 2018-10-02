# Copyright 2018 HTCondor Team, Computer Sciences Department,
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

from typing import Iterable, Dict, Union, Optional, Callable, Any
import logging

from . import mapping, options, result

logger = logging.getLogger(__name__)


class MappedFunction:
    def __init__(
        self,
        func: Callable,
        map_options: Optional[options.MapOptions] = None
    ):
        """
        Parameters
        ----------
        func
            A function to wrap in a :class:`MappedFunction`.
        map_options
            An instance of :class:`htmap.MapOptions`.
            Any map calls from the :class:`MappedFunction` produced by this decorator will inherit from this.
        """
        self.func = func

        if map_options is None:
            map_options = options.MapOptions()
        self.map_options = map_options

        logger.debug(f'initialized mapped function for {self.func} with options {self.map_options}')

    def __repr__(self):
        return f'<{self.__class__.__name__}(func = {self.func}, map_options = {self.map_options})>'

    def __call__(self, *args, **kwargs):
        """Call the function as normal, locally."""
        return self.func(*args, **kwargs)

    def map(
        self,
        map_id: str,
        args: Iterable[Any],
        force_overwrite: bool = False,
        map_options: Optional[options.MapOptions] = None,
        **kwargs,
    ) -> result.MapResult:
        """
        Map a function call over a one-dimensional iterable of arguments.
        The function must take a single positional argument and any number of keyword arguments.

        The same keyword arguments are passed to *each call*, not mapped over.

        Parameters
        ----------
        map_id
            The ``map_id`` to assign to this map.
        args
            An iterable of arguments to pass to the mapped function.
        kwargs
            Any additional keyword arguments are passed as keyword arguments to the mapped function.
        force_overwrite
            If ``True``, and there is already a map with the given ``map_id``, it will be removed before running this one.
        map_options
            An instance of :class:`htmap.MapOptions`.

        Returns
        -------
        result :
            A :class:`htmap.MapResult` representing the map.
        """
        if map_options is None:
            map_options = options.MapOptions()

        return mapping.map(
            map_id = map_id,
            func = self.func,
            args = args,
            force_overwrite = force_overwrite,
            map_options = options.MapOptions.merge(map_options, self.map_options),
            **kwargs,
        )

    def starmap(
        self,
        map_id: str,
        args: Optional[Iterable[tuple]] = None,
        kwargs: Optional[Iterable[Dict[str, Any]]] = None,
        force_overwrite: bool = False,
        map_options: Optional[options.MapOptions] = None,
    ) -> result.MapResult:
        """
        Map a function call over aligned iterables of arguments and keyword arguments.
        Each element of ``args`` and ``kwargs`` is unpacked into the signature of the function, so their elements should be tuples and dictionaries corresponding to position and keyword arguments of the mapped function.

        Parameters
        ----------
        map_id
            The ``map_id`` to assign to this map.
        args
            An iterable of tuples of positional arguments to unpack into the mapped function.
        kwargs
            An iterable of dictionaries of keyword arguments to unpack into the mapped function.
        force_overwrite
            If ``True``, and there is already a map with the given ``map_id``, it will be removed before running this one.
        map_options
            An instance of :class:`htmap.MapOptions`.

        Returns
        -------
        result :
            A :class:`htmap.MapResult` representing the map.
        """
        if map_options is None:
            map_options = options.MapOptions()

        return mapping.starmap(
            map_id = map_id,
            func = self.func,
            args = args,
            kwargs = kwargs,
            force_overwrite = force_overwrite,
            map_options = options.MapOptions.merge(map_options, self.map_options),
        )

    def build_map(
        self,
        map_id: str,
        force_overwrite: bool = False,
        map_options: Optional[options.MapOptions] = None,
    ) -> mapping.MapBuilder:
        """
        Return a :class:`htmap.MapBuilder` for the wrapped function.

        Parameters
        ----------
        map_id
            The ``map_id`` to assign to this map.
        force_overwrite
            If ``True``, and there is already a map with the given ``map_id``, it will be removed before running this one.
        map_options
            An instance of :class:`htmap.MapOptions`.

        Returns
        -------
        map_builder :
            A :class:`htmap.MapBuilder` for the wrapped function.
        """
        if map_options is None:
            map_options = options.MapOptions()

        return mapping.build_map(
            map_id = map_id,
            func = self.func,
            force_overwrite = force_overwrite,
            map_options = options.MapOptions.merge(map_options, self.map_options),
        )


def htmap(map_options: Optional[options.MapOptions] = None) -> Union[Callable, MappedFunction]:
    """
    A decorator that wraps a function in an :class:`MappedFunction`,
    which provides an interface for mapping functions calls out to an HTCondor cluster.

    Parameters
    ----------
    map_options
        An instance of :class:`htmap.MapOptions`.
        Any map calls from the :class:`MappedFunction` produced by this decorator will inherit from this.

    Returns
    -------
    mapped_function
        A :class:`MappedFunction` that wraps the function (or a wrapper function that does the wrapping).
    """
    if map_options is None:  # call with parens but no args
        def wrapper(func: Callable) -> MappedFunction:
            return MappedFunction(func)

        return wrapper

    elif callable(map_options):  # call with no parens on function
        return MappedFunction(map_options)

    elif isinstance(map_options, options.MapOptions):  # call with map options
        def wrapper(func: Callable) -> MappedFunction:
            return MappedFunction(func, map_options = map_options)

        return wrapper
