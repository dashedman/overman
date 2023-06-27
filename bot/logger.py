import logging

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
coloredlogs.install(
    level=logging.DEBUG,
    fmt='[%(asctime)s|%(name)s|%(levelname)7s] %(message)s',
    level_styles=level_colormap,
    field_styles=field_colormap,
)

getLogger = logging.getLogger
