"""Error handlers for UWS and DALI services.

Currently these error handlers return ``text/plain`` errors.  VOTable errors
may be a better choice, but revision 1.0 of the SODA standard only allows
``text/plain`` errors for sync routes.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse

from .exceptions import UWSError

__all__ = ["install_error_handlers"]


async def _uws_error_handler(
    request: Request, exc: UWSError
) -> PlainTextResponse:
    response = f"{exc.error_code.value} {str(exc)}\n"
    if exc.detail:
        response += "\n{exc.detail}"
    return PlainTextResponse(response, status_code=exc.status_code)


async def _usage_handler(
    request: Request, exc: RequestValidationError
) -> PlainTextResponse:
    return PlainTextResponse(f"UsageError\n\n{str(exc)}", status_code=422)


def install_error_handlers(app: FastAPI) -> None:
    """Install error handlers that follow DALI and UWS conventions.

    This function must be called during application setup for any FastAPI app
    using the UWS layer for correct error message handling.  Be aware that
    this will change the error response for all parameter validation errors
    from FastAPI.
    """
    app.exception_handler(UWSError)(_uws_error_handler)
    app.exception_handler(RequestValidationError)(_usage_handler)
