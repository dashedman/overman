class OvermanException(Exception):
    pass


class RequestException(OvermanException):
    pass


class BalanceInsufficientError(RequestException):
    pass


class OrderSizeTooSmallError(RequestException):
    pass


class OrderCanceledError(RequestException):
    pass

