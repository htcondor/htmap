class HTCMapException(Exception):
    """Base exception for all ``htcmap`` exceptions."""
    pass


class MissingSetting(HTCMapException):
    """The requested setting has not been set."""
    pass


class HashNotInResult(HTCMapException):
    """The given hash is not in this :class:`htcmap.MapResult`."""
    pass


class OutputNotFound(HTCMapException):
    """The output file that was requested does not exist."""
    pass


class NoResultYet(HTCMapException):
    """The :class:`htcmap.JobBuilder` does not have an associated :class:`htcmap.MapResult` yet."""
    pass


class TimeoutError(TimeoutError, HTCMapException):
    """An operation has timed out because it took too long."""
    pass
