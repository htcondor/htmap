import enum
import itertools

integer_generator = itertools.count(0)


class Submit:
    def __init__(self, submit_dictionary):
        self.submit_dictionary = submit_dictionary

    def queue_with_itemdata(self, *args):
        return SubmitResult(next(integer_generator))


class SubmitResult:
    def __init__(self, clusterid):
        self.clusterid = clusterid

    def cluster(self):
        return self.clusterid


class Schedd:
    def transaction(self):
        return Transaction()


class Transaction:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class JobAction(enum.Enum):
    Remove = 'remove'
