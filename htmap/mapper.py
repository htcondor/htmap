from typing import Tuple, Iterable, Dict, Union, Optional, Callable, Any

from . import mapping, options, result, exceptions


class MappedFunction:
    def __init__(
        self,
        func: Callable,
        map_options: Optional[options.MapOptions] = None
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
        map_options: Optional[options.MapOptions] = None,
        **kwargs,
    ) -> result.MapResult:
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
        args: Optional[Iterable[Tuple]] = None,
        kwargs: Optional[Iterable[Dict]] = None,
        force_overwrite: bool = False,
        map_options: Optional[options.MapOptions] = None,
    ) -> result.MapResult:
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

    Returns
    -------
    mapper
        An :class:`MappedFunction` that wraps the function (or a wrapper function that does the wrapping).
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
