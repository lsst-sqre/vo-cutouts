"""Handlers for the app's external root, ``/utout``.

UWS provides general handlers for everything it can, but POST at the top level
to create a new job has to be provided by the application since only the
application knows the job parameters.
"""

from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, Form, Query, Request, Response
from fastapi.responses import PlainTextResponse, RedirectResponse
from safir.metadata import get_metadata

from ..config import config
from ..dependencies.auth import auth_dependency
from ..models.index import Index
from ..uws.dependencies import (
    UWSFactory,
    uws_dependency,
    uws_params_dependency,
)
from ..uws.exceptions import ParameterError
from ..uws.handlers import uws_router
from ..uws.models import ExecutionPhase, JobParameter

__all__ = ["external_router"]

external_router = APIRouter()
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


@external_router.get(
    "/",
    response_model=Index,
    response_model_exclude_none=True,
    summary="Application metadata",
)
async def get_index() -> Index:
    """GET ``/cutout/`` (the app's external root).

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


@external_router.get(
    "/availability",
    description="VOSI-availability resource for the image cutout service",
    responses={200: {"content": {"application/xml": {}}}},
    summary="IVOA service availability",
)
async def get_availability(
    request: Request, uws_factory: UWSFactory = Depends(uws_dependency)
) -> Response:
    job_service = uws_factory.create_job_service()
    availability = await job_service.availability()
    templates = uws_factory.create_templates()
    return templates.availability(request, availability)


@external_router.get(
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


async def _sync_request(
    params: List[JobParameter], user: str, uws_factory: UWSFactory
) -> Response:
    """Process a sync request.

    Shared code for the GET and POST methods.
    """
    runid = None
    for param in params:
        if param.parameter_id == "runid":
            runid = param.value
    params = [p for p in params if p.parameter_id != "runid"]

    # Create the job, start it, and wait for it to complete.
    job_service = uws_factory.create_job_service()
    job = await job_service.create(user, run_id=runid, params=params)
    await job_service.start(user, job.job_id)
    job = await job_service.get(
        user, job.job_id, wait=config.sync_timeout, wait_for_completion=True
    )

    # Check for error states.
    if job.phase not in (ExecutionPhase.COMPLETED, ExecutionPhase.ERROR):
        return PlainTextResponse(
            f"Error\n\nCutout did not complete in {config.sync_timeout}s",
            status_code=400,
        )
    if not job.results:
        return PlainTextResponse(
            "Error\n\nJob did not return any results", status_code=400
        )
    if job.error:
        if job.error.detail:
            return PlainTextResponse(
                f"Error {job.error.message}\n\n{job.error.detail}",
                status_code=400,
            )
        else:
            return PlainTextResponse(
                f"Error\n\n{job.error.message}", status_code=400
            )

    # Redirect to the URL of the first result.
    return RedirectResponse(job.results[0].url, status_code=303)


@external_router.get(
    "/sync",
    description=(
        "Synchronously request a cutout. This will wait for the cutout to be"
        " completed and return the resulting image as a FITS file. (The image"
        " will be returned via a redirect to a URL at the underlying object"
        " store.)"
    ),
    responses={
        303: {"description": "Redirect to result of successful cutout"},
        400: {
            "description": "Cutout job failed",
            "content": {"text/plain": {}},
        },
    },
    status_code=303,
    summary="Synchronous cutout",
)
async def get_sync(
    request: Request,
    id: List[str] = Query(
        ...,
        title="Source ID",
        description=(
            "Identifiers of images from which to make a cutout. This"
            " parameter is mandatory."
        ),
    ),
    pos: Optional[List[str]] = Query(
        None,
        title="Cutout positions",
        description=(
            "Positions to cut out. Supported parameters are RANGE followed"
            " by min and max ra and min and max dec; CIRCLE followed by"
            " ra, dec, and radius; and POLYGON followed by a list of"
            " ra/dec positions for vertices. Arguments must be separated"
            " by spaces and parameters are double-precision floating point"
            " numbers expressed as strings."
        ),
    ),
    circle: Optional[List[str]] = Query(
        None,
        title="Cutout circle positions",
        description=(
            "Circles to cut out. The value must be the ra and dec of the"
            " center of the circle and then the radius, as"
            " double-precision floating point numbers expressed as"
            " strings and separated by spaces."
        ),
    ),
    polygon: Optional[List[str]] = Query(
        None,
        title="Cutout polygon positions",
        description=(
            "Polygons to cut out. The value must be ra/dec pairs for each"
            " vertex, ordered so that the polygon winding direction is"
            " counter-clockwise (when viewed from the origin towards the"
            " sky). These parameters are double-precision floating point"
            " numbers expressed as strings and separated by spaces."
        ),
    ),
    runid: Optional[str] = Query(
        None,
        title="Run ID for job",
        description=(
            "An opaque string that is returned in the job metadata and"
            " job listings. Maybe used by the client to associate jobs"
            " with specific larger operations."
        ),
    ),
    params: List[JobParameter] = Depends(uws_params_dependency),
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> Response:
    return await _sync_request(params, user, uws_factory)


@external_router.post(
    "/sync",
    description=(
        "Synchronously request a cutout. This will wait for the cutout to be"
        " completed and return the resulting image as a FITS file. (The image"
        " will be returned via a redirect to a URL at the underlying object"
        " store.)"
    ),
    responses={
        303: {"description": "Redirect to result of successful cutout"},
        400: {
            "description": "Cutout job failed",
            "content": {"text/plain": {}},
        },
    },
    status_code=303,
    summary="Synchronous cutout",
)
async def post_sync(
    request: Request,
    id: Optional[List[str]] = Form(
        None,
        title="Source ID",
        description=(
            "Identifiers of images from which to make a cutout. This"
            " parameter is mandatory."
        ),
    ),
    pos: Optional[List[str]] = Form(
        None,
        title="Cutout positions",
        description=(
            "Positions to cut out. Supported parameters are RANGE followed"
            " by min and max ra and min and max dec; CIRCLE followed by"
            " ra, dec, and radius; and POLYGON followed by a list of"
            " ra/dec positions for vertices. Arguments must be separated"
            " by spaces and parameters are double-precision floating point"
            " numbers expressed as strings."
        ),
    ),
    circle: Optional[List[str]] = Form(
        None,
        title="Cutout circle positions",
        description=(
            "Circles to cut out. The value must be the ra and dec of the"
            " center of the circle and then the radius, as"
            " double-precision floating point numbers expressed as"
            " strings and separated by spaces."
        ),
    ),
    polygon: Optional[List[str]] = Form(
        None,
        title="Cutout polygon positions",
        description=(
            "Polygons to cut out. The value must be ra/dec pairs for each"
            " vertex, ordered so that the polygon winding direction is"
            " counter-clockwise (when viewed from the origin towards the"
            " sky). These parameters are double-precision floating point"
            " numbers expressed as strings and separated by spaces."
        ),
    ),
    runid: Optional[str] = Form(
        None,
        title="Run ID for job",
        description=(
            "An opaque string that is returned in the job metadata and"
            " job listings. Maybe used by the client to associate jobs"
            " with specific larger operations."
        ),
    ),
    params: List[JobParameter] = Depends(uws_params_dependency),
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> Response:
    return await _sync_request(params, user, uws_factory)


@uws_router.post(
    "",
    description="Create a new image cutout job.",
    response_class=RedirectResponse,
    status_code=303,
    summary="Create async job",
)
async def create_job(
    request: Request,
    id: Optional[List[str]] = Form(
        None,
        title="Source ID",
        description=(
            "Identifiers of images from which to make a cutout. This"
            " parameter is mandatory."
        ),
    ),
    pos: Optional[List[str]] = Form(
        None,
        title="Cutout positions",
        description=(
            "Positions to cut out. Supported parameters are RANGE followed"
            " by min and max ra and min and max dec; CIRCLE followed by"
            " ra, dec, and radius; and POLYGON followed by a list of"
            " ra/dec positions for vertices. Arguments must be separated"
            " by spaces and parameters are double-precision floating point"
            " numbers expressed as strings."
        ),
    ),
    circle: Optional[List[str]] = Form(
        None,
        title="Cutout circle positions",
        description=(
            "Circles to cut out. The value must be the ra and dec of the"
            " center of the circle and then the radius, as"
            " double-precision floating point numbers expressed as"
            " strings and separated by spaces."
        ),
    ),
    polygon: Optional[List[str]] = Form(
        None,
        title="Cutout polygon positions",
        description=(
            "Polygons to cut out. The value must be ra/dec pairs for each"
            " vertex, ordered so that the polygon winding direction is"
            " counter-clockwise (when viewed from the origin towards the"
            " sky). These parameters are double-precision floating point"
            " numbers expressed as strings and separated by spaces."
        ),
    ),
    phase: Optional[Literal["RUN"]] = Form(
        None, title="Immediately start job"
    ),
    runid: Optional[str] = Form(
        None,
        title="Run ID for job",
        description=(
            "An opaque string that is returned in the job metadata and"
            " job listings. Maybe used by the client to associate jobs"
            " with specific larger operations."
        ),
    ),
    params: List[JobParameter] = Depends(uws_params_dependency),
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    autorun = False
    run_id = None
    for param in params:
        if param.parameter_id == "phase":
            if param.value != "RUN":
                raise ParameterError(f"Invalid phase {param.value}")
            autorun = True
        elif param.parameter_id == "runid":
            run_id = param.value
    params = [p for p in params if p.parameter_id not in ("phase", "runid")]

    # Create the job and optionally start it.
    job_service = uws_factory.create_job_service()
    job = await job_service.create(user, run_id=run_id, params=params)
    if autorun:
        await job_service.start(user, job.job_id)

    # Redirect to the new job.
    return request.url_for("get_job", job_id=job.job_id)


# Add the UWS routes to our external routes.  This must be done after defining
# the POST handler for /jobs because of oddities in the implementation details
# of include_router.
external_router.include_router(uws_router, prefix="/jobs")
