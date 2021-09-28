"""Middleware for UWS services."""

from typing import Awaitable, Callable
from urllib.parse import urlencode

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


class CaseInsensitiveQueryMiddleware(BaseHTTPMiddleware):
    """Make query parameter keys all lowercase.

    Unfortunately, UWS requires that query parameters be case-insensitive,
    which is not supported by modern HTTP web frameworks.  This middleware
    attempts to work around this by lowercasing the query parameter keys
    before the request is processed, allowing normal FastAPI query parsing to
    then work without regard for case.

    Based on https://github.com/tiangolo/fastapi/issues/826.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        params = [(k.lower(), v) for k, v in request.query_params.items()]
        request.scope["query_string"] = urlencode(params).encode()
        return await call_next(request)
