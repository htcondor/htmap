class HTMapException(Exception):
    """Base exception for all ``htmap`` exceptions."""
    pass


class MissingSetting(HTMapException):
    """The requested setting has not been set."""
    pass


class OutputNotFound(HTMapException):
    """The output file that was requested does not exist."""
    pass


class NoResultYet(HTMapException):
    """The :class:`htmap.MapBuilder` does not have an associated :class:`htmap.MapResult` yet."""
    pass


class TimeoutError(TimeoutError, HTMapException):
    """An operation has timed out because it took too long."""
    pass


class MapIDAlreadyExists(HTMapException):
    pass


class MapIDNotFound(HTMapException):
    pass
