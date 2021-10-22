"""Error handlers for UWS and DALI services.

Currently these error handlers return ``text/plain`` errors.  VOTable errors
may be a better choice, but revision 1.0 of the SODA standard only allows
``text/plain`` errors for sync routes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse

from .exceptions import DataMissingError, PermissionDeniedError, UsageError

if TYPE_CHECKING:
    from typing import Union

    from fastapi import FastAPI, Request

__all__ = ["install_error_handlers"]


async def _data_missing_handler(
    request: Request, exc: DataMissingError
) -> PlainTextResponse:
    return PlainTextResponse(f"UsageError\n\n{str(exc)}", status_code=404)


async def _permission_denied_handler(
    request: Request, exc: PermissionDeniedError
) -> PlainTextResponse:
    return PlainTextResponse("AuthorizationError", status_code=403)


async def _usage_handler(
    request: Request, exc: Union[RequestValidationError, UsageError]
) -> PlainTextResponse:
    return PlainTextResponse(f"UsageError\n\n{str(exc)}", status_code=422)


def install_error_handlers(app: FastAPI) -> None:
    """Install error handlers that follow DALI and UWS conventions.

    This function must be called during application setup for any FastAPI app
    using the UWS layer for correct error message handling.  Be aware that
    this will change the error response for all parameter validation errors
    from FastAPI.
    """
    app.exception_handler(DataMissingError)(_data_missing_handler)
    app.exception_handler(PermissionDeniedError)(_permission_denied_handler)
    app.exception_handler(RequestValidationError)(_usage_handler)
    app.exception_handler(UsageError)(_usage_handler)
