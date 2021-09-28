"""Gafaelfawr authentication dependencies."""

from fastapi import Header


async def auth_dependency(x_auth_request_user: str = Header(...)) -> str:
    """Retrieve authentication information from HTTP headers.

    Intended for use with applications protected by Gafaelfawr, this retrieves
    authentication information from headers added to the incoming request by
    the Gafaelfawr ``auth_request`` NGINX subhandler.
    """
    return x_auth_request_user
