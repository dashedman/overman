import logging
import os
from logging.handlers import RotatingFileHandler

import coloredlogs as coloredlogs


class Color:
    """ https://coloredlogs.readthedocs.io/en/latest/api.html#id28 """
    GREY = 8
    ORANGE = 214


level_colormap = {
    'critical': {'bold': True, 'color': 'red'},
    'debug': {'color': 'white', 'faint': True},
    'error': {'color': 'red'},
    'info': {'color': 'cyan'},
    'notice': {'color': 'magenta'},
    'spam': {'color': 'green', 'faint': True},
    'success': {'bold': True, 'color': 'green'},
    'verbose': {'color': 'blue'},
    'warning': {'color': Color.ORANGE}
}
field_colormap = {
    'asctime': {'color': 'green'},
    'hostname': {'color': 'magenta'},
    'levelname': {'bold': True, 'color': 'yellow'},
    'name': {'color': 'blue'},
    'programname': {'color': 'cyan'},
    'username': {'color': 'yellow'}
}

FORMATTER_STR = '[%(asctime)s|%(name)s|%(levelname)7s] %(message)s'


def setup_logger(logger_name, logger_level=logging.INFO, with_root: bool = False):
    setup_logger = logging.getLogger(None if with_root else logger_name)

    log_dir = '../logs'
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    log_file = os.path.join(log_dir, f'{logger_name}.log')
    file_handler = RotatingFileHandler(
        log_file,
        "a",
        1024 * 1024,
        10,
        encoding='utf-8',
    )
    file_handler.setFormatter(logging.Formatter(FORMATTER_STR))
    setup_logger.addHandler(file_handler)

    coloredlogs.install(
        logger=setup_logger,
        level=logger_level,
        fmt=FORMATTER_STR,
        level_styles=level_colormap,
        field_styles=field_colormap,
        isatty=True,
    )
    return logging.getLogger(logger_name)
