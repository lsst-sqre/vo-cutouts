"""Handlers for the app's external root, ``/api/cutout``.

UWS provides general handlers for everything it can, but POST at the top level
to create a new job has to be provided by the application since only the
application knows the job parameters.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response
from safir.metadata import get_metadata
from safir.slack.webhook import SlackRouteErrorHandler

from ..config import config
from ..models.index import Index
from ..uws.dependencies import UWSFactory, uws_dependency

router = APIRouter(route_class=SlackRouteErrorHandler)
"""FastAPI router for all external handlers."""

_CAPABILITIES_TEMPLATE = """
<?xml version="1.0"?>
<capabilities
    xmlns:vosi="http://www.ivoa.net/xml/VOSICapabilities/v1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:vod="http://www.ivoa.net/xml/VODataService/v1.1">
  <capability standardID="ivo://ivoa.net/std/VOSI#capabilities">
    <interface xsi:type="vod:ParamHTTP" version="1.0">
      <accessURL use="full">{capabilities_url}</accessURL>
    </interface>
  </capability>
  <capability standardID="ivo://ivoa.net/std/VOSI#availability">
    <interface xsi:type="vod:ParamHTTP" version="1.0">
      <accessURL use="full">{availability_url}</accessURL>
    </interface>
  </capability>
  <capability standardid="ivo://ivoa.net/std/SODA#sync-1.0">
    <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
      <accessURL use="full">{sync_url}</accessURL>
    </interface>
  </capability>
  <capability standardid="ivo://ivoa.net/std/SODA#async-1.0">
    <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
      <accessURL use="full">{async_url}</accessURL>
    </interface>
  </capability>
</capabilities>
"""

__all__ = ["router"]


@router.get(
    "/",
    response_model=Index,
    response_model_exclude_none=True,
    summary="Application metadata",
)
async def get_index() -> Index:
    """GET ``/api/cutout/`` (the app's external root).

    Customize this handler to return whatever the top-level resource of your
    application should return. For example, consider listing key API URLs.
    When doing so, also change or customize the response model in
    `vocutouts.models.Index`.

    By convention, the root of the external API includes a field called
    ``metadata`` that provides the same Safir-generated metadata as the
    internal root endpoint.
    """
    metadata = get_metadata(
        package_name="vo-cutouts",
        application_name=config.name,
    )
    return Index(metadata=metadata)


@router.get(
    "/availability",
    description="VOSI-availability resource for the image cutout service",
    responses={200: {"content": {"application/xml": {}}}},
    summary="IVOA service availability",
)
async def get_availability(
    request: Request,
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
) -> Response:
    job_service = uws_factory.create_job_service()
    availability = await job_service.availability()
    templates = uws_factory.create_templates()
    return templates.availability(request, availability)


@router.get(
    "/capabilities",
    description="VOSI-capabilities resource for the image cutout service",
    responses={200: {"content": {"application/xml": {}}}},
    summary="IVOA service capabilities",
)
async def get_capabilities(request: Request) -> Response:
    result = _CAPABILITIES_TEMPLATE.strip().format(
        availability_url=request.url_for("get_availability"),
        capabilities_url=request.url_for("get_capabilities"),
        sync_url=request.url_for("post_sync"),
        async_url=request.url_for("create_job"),
    )
    return Response(content=result, media_type="application/xml")
