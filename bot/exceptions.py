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


class PredictionException(OvermanException):
    def __init__(self, reason: str):
        self.reason = reason
