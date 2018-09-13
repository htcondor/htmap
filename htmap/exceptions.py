class HTMapException(Exception):
    """Base exception for all ``htmap`` exceptions."""
    pass


class TimeoutError(TimeoutError, HTMapException):
    """An operation has timed out because it took too long."""
    pass


class MissingSetting(HTMapException):
    """The requested setting has not been set."""
    pass


class OutputNotFound(HTMapException):
    """The requested output file does not exist."""
    pass


class NoResultYet(HTMapException):
    """The :class:`htmap.MapBuilder` does not have an associated :class:`htmap.MapResult` yet because it is still inside the ``with`` block."""
    pass


class MapIdAlreadyExists(HTMapException):
    """The requested ``map_id`` already exists (recover the :class:`MapResult`, then either use or delete it)."""
    pass


class InvalidMapId(HTMapException):
    """The ``map_id`` has an invalid character in it."""
    pass


class MapIdNotFound(HTMapException):
    """The requested ``map_id`` does not exist."""
    pass


class EmptyMap(HTMapException):
    """The map contains no inputs."""
    pass


class ReservedOptionKeyword(HTMapException):
    """The map option keyword you tried to use is reserved by HTMap for internal use."""
    pass


class MisalignedInputData(HTMapException):
    """There is some kind of mismatch between the lengths of the function arguments and the variadic map options."""
    pass


class CannotRenameMap(HTMapException):
    """The map cannot be renamed right now."""
    pass
