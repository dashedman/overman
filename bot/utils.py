import itertools
from itertools import islice
from typing import Iterable


def chunk(raw_list: Iterable[str], size=10):
    raw_list = iter(raw_list)
    return list(iter(lambda: tuple(islice(raw_list, size)), ()))


def join_multiline_strings(*strings: str, with_filling_space: bool = False) -> str:
    strings_lines = [s.split('\n') for s in strings]
    max_strings_width = [max(map(len, lines)) for lines in strings_lines]
    result_lines = []
    for one_line_strings in itertools.zip_longest(*strings_lines, fillvalue=''):
        if with_filling_space:
            result_line = ''.join((
                s + ' ' * (need_length - len(s))
                for s, need_length in zip(one_line_strings, max_strings_width)
            ))
        else:
            result_line = ''.join(one_line_strings)
        result_lines.append(result_line)
    return '\n'.join(result_lines)
