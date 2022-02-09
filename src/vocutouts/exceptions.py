"""Exceptions for the image cutout service."""

from __future__ import annotations

from typing import List

from .uws.exceptions import ParameterError
from .uws.models import JobParameter

__all__ = ["InvalidCutoutParameterError"]


class InvalidCutoutParameterError(ParameterError):
    """The parameters for the cutout were invalid."""

    def __init__(self, message: str, params: List[JobParameter]) -> None:
        detail = "\n".join(f"{p.parameter_id}={p.value}" for p in params)
        super().__init__(message, detail)
        self.params = params
