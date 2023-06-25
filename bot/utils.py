from itertools import islice
from typing import Iterable


def chunk(raw_list: Iterable[str], size=10):
    raw_list = iter(raw_list)
    return list(iter(lambda: tuple(islice(raw_list, size)), ()))
