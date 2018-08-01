class HTMapException(Exception):
    """Base exception for all ``htmap`` exceptions."""
    pass


class MissingSetting(HTMapException):
    """The requested setting has not been set."""
    pass


class HashNotInResult(HTMapException):
    """The given hash is not in this :class:`htmap.MapResult`."""
    pass


class OutputNotFound(HTMapException):
    """The output file that was requested does not exist."""
    pass


class NoResultYet(HTMapException):
    """The :class:`htmap.JobBuilder` does not have an associated :class:`htmap.MapResult` yet."""
    pass


class TimeoutError(TimeoutError, HTMapException):
    """An operation has timed out because it took too long."""
    pass
