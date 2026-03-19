"""Exceptions for the vo-cutouts service."""

from safir.uws import UWSError

__all__ = ["MultiValuedParameterError"]


class MultiValuedParameterError(UWSError):
    """Multiple values not allowed for this parameter."""

    def __init__(self, message: str) -> None:
        super().__init__("MultiValuedParamNotSupported", message)
        self.status_code = 422
