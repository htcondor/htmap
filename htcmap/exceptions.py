class HTCMapException(Exception):
    pass


class MissingSetting(HTCMapException):
    pass


class HashNotInResult(HTCMapException):
    pass


class OutputNotFound(HTCMapException):
    pass


class NoResultYet(HTCMapException):
    pass


class TimeoutError(TimeoutError, HTCMapException):
    pass
