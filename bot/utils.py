import itertools
from itertools import islice
from typing import Iterable


def chunk(raw_list: Iterable[str], size=10):
    raw_list = iter(raw_list)
    return list(iter(lambda: tuple(islice(raw_list, size)), ()))


def join_multiline_strings(*strings: str) -> str:
    strings_lines = [s.split('\n') for s in strings]
    result_lines = []
    for one_line_strings in itertools.zip_longest(*strings_lines, fillvalue=''):
        result_lines.append(''.join(one_line_strings))
    return '\n'.join(result_lines)
