class OvermanException(Exception):
    pass


class RequestException(OvermanException):
    pass


class BalanceInsufficientError(RequestException):
    pass


class OrderSizeTooSmallError(RequestException):
    pass


class OrderCanceledError(RequestException):
    def __init__(self, msg, size=None):
        self.msg = msg
        self.size = size

    def __str__(self):
        return self.msg + f'(size: {self.size})'


class PredictionException(OvermanException):
    def __init__(self, reason: str):
        self.reason = reason
