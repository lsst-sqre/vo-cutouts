"""Internal HTTP handlers that serve relative to the root path, ``/``.

These handlers aren't externally visible since the app is available at a path,
``/vocutouts``. See `vocutouts.handlers.external` for the external endpoint
handlers.

These handlers should be used for monitoring, health checks, internal status,
or other information that should not be visible outside the Kubernetes cluster.
"""

from fastapi import APIRouter
from safir.metadata import Metadata, get_metadata
from safir.slack.webhook import SlackRouteErrorHandler

from ..config import config

router = APIRouter(route_class=SlackRouteErrorHandler)
"""FastAPI router for all internal handlers."""

__all__ = ["router"]


@router.get(
    "/",
    description=(
        "Return metadata about the running application. Can also be used as"
        " a health check. This route is not exposed outside the cluster and"
        " therefore cannot be used by external clients."
    ),
    response_model_exclude_none=True,
    summary="Application metadata",
)
async def get_index() -> Metadata:
    return get_metadata(
        package_name="vo-cutouts", application_name=config.name
    )
