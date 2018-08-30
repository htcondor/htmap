import enum
import itertools

integer_generator = itertools.count(0)


class Submit:
    def __init__(self, submit_dictionary):
        self.submit_dictionary = submit_dictionary

    def __iter__(self):
        yield from self.submit_dictionary.items()


class SubmitResult:
    def __init__(self):
        self.clusterid = next(integer_generator)

    def cluster(self):
        return self.clusterid


class Schedd:
    def transaction(self):
        return Transaction()

    def act(self, *args, **kwargs):
        return {}


class Transaction:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class JobAction(enum.Enum):
    Remove = 'remove'
