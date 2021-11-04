"""Handlers for the UWS API to a service.

These handlers should be reusable for any IVOA service that implements UWS.
The user of these handlers must provide an additional handler for POST at the
root of the job list, since that handler has to specify the input parameters
for a job, which will vary by service.

Notes
-----
To use these handlers, include the ``uws_router`` in an appropriate FastAPI
router, generally with a prefix matching the URL root for the async API.  For
example:

.. code-block:: python

   external_router.include_router(uws_router, prefix="/jobs")
"""

from datetime import datetime
from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, Form, Query, Request, Response
from fastapi.responses import PlainTextResponse, RedirectResponse

from ..dependencies.auth import auth_dependency
from .dependencies import (
    UWSFactory,
    uws_dependency,
    uws_post_params_dependency,
)
from .exceptions import DataMissingError, ParameterError, PermissionDeniedError
from .models import ExecutionPhase, JobParameter
from .utils import isodatetime, parse_isodatetime

__all__ = ["uws_router"]

uws_router = APIRouter()
"""FastAPI router for all external handlers."""


@uws_router.get(
    "",
    description=(
        "List all existing jobs for the current user. Jobs will be sorted"
        " by creation date, with the most recently created listed first."
    ),
    responses={200: {"content": {"application/xml": {}}}},
    summary="Async job list",
)
async def get_job_list(
    request: Request,
    phase: Optional[List[ExecutionPhase]] = Query(
        None,
        title="Execution phase",
        description="Limit results to the provided execution phases",
    ),
    after: Optional[datetime] = Query(
        None,
        title="Creation date",
        description="Limit results to jobs created after this date",
    ),
    last: Optional[int] = Query(
        None,
        title="Number of jobs",
        description="Return at most the given number of jobs",
    ),
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> Response:
    job_service = uws_factory.create_job_service()
    jobs = await job_service.list_jobs(
        user, phases=phase, after=after, count=last
    )
    base_url = request.url_for("get_job_list")
    templates = uws_factory.create_templates()
    return templates.job_list(request, jobs, base_url)


@uws_router.get(
    "/{job_id}",
    responses={200: {"content": {"application/xml": {}}}},
    summary="Job details",
)
async def get_job(
    job_id: str,
    request: Request,
    wait: int = Query(
        None,
        title="Wait for status changes",
        description=(
            "Maximum number of seconds to wait or -1 to wait for as long as"
            " the server permits"
        ),
    ),
    phase: ExecutionPhase = Query(
        None,
        title="Initial phase for waiting",
        description=(
            "When waiting for status changes, consider this to be the initial"
            " execution phase. If the phase has already changed, return"
            " immediately. This parameter should always be provided when"
            " wait is used."
        ),
    ),
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> Response:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(user, job_id, wait=wait, wait_phase=phase)
    templates = uws_factory.create_templates()
    return await templates.job(request, job)


@uws_router.delete(
    "/{job_id}",
    status_code=303,
    response_class=RedirectResponse,
    summary="Delete a job",
)
async def delete_job(
    job_id: str,
    request: Request,
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    job_service = uws_factory.create_job_service()
    await job_service.delete(user, job_id)
    return request.url_for("get_job_list")


@uws_router.post(
    "/{job_id}",
    description=(
        "Alternate job deletion mechanism for clients that cannot use DELETE."
    ),
    response_class=RedirectResponse,
    status_code=303,
    summary="Delete a job",
)
async def delete_job_via_post(
    job_id: str,
    request: Request,
    action: Optional[Literal["DELETE"]] = Form(
        None,
        title="Action to perform",
        description="Mandatory, must be set to DELETE",
    ),
    params: List[JobParameter] = Depends(uws_post_params_dependency),
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    # Work around the obnoxious requirement for case-insensitive parameters,
    # which is also why the action parameter is declared as optional (but is
    # listed to help with API documentation generation).
    saw_delete = False
    for param in params:
        if param.parameter_id != "action" or param.value != "DELETE":
            msg = f"Unknown parameter {param.parameter_id}={param.value}"
            raise ParameterError(msg)
        if param.parameter_id == "action" and param.value == "DELETE":
            saw_delete = True
    if not saw_delete:
        raise ParameterError("No action given")

    # Do the actual deletion.
    job_service = uws_factory.create_job_service()
    await job_service.delete(user, job_id)
    return request.url_for("get_job_list")


@uws_router.get(
    "/{job_id}/destruction",
    response_class=PlainTextResponse,
    summary="Destruction time for job",
)
async def get_job_destruction(
    job_id: str,
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(user, job_id)
    return isodatetime(job.destruction_time)


@uws_router.post(
    "/{job_id}/destruction",
    response_class=RedirectResponse,
    status_code=303,
    summary="Change job destruction time",
)
async def post_job_destruction(
    job_id: str,
    request: Request,
    destruction: Optional[datetime] = Form(
        None,
        title="New destruction time",
        description="Must be in ISO 8601 format.",
        example="2021-09-10T10:01:02Z",
    ),
    params: List[JobParameter] = Depends(uws_post_params_dependency),
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    # Work around the obnoxious requirement for case-insensitive parameters.
    for param in params:
        if param.parameter_id != "destruction":
            msg = f"Unknown parameter {param.parameter_id}={param.value}"
            raise ParameterError(msg)
        destruction = parse_isodatetime(param.value)
        if destruction is None:
            raise ParameterError(f"Invalid date {param.value}")
    if not destruction:
        raise ParameterError("No new destruction time given")

    # Update the destruction time.  Note that the policy layer may modify the
    # destruction time, so the time set may not match the input.
    job_service = uws_factory.create_job_service()
    await job_service.update_destruction(user, job_id, destruction)
    return request.url_for("get_job", job_id=job_id)


@uws_router.get(
    "/{job_id}/error",
    responses={
        200: {"content": {"application/xml": {}}},
        404: {"description": "Job not found or job did not fail"},
    },
    summary="Job error",
)
async def get_job_error(
    job_id: str,
    request: Request,
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> Response:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(user, job_id)
    if not job.error:
        raise DataMissingError(f"Job {job_id} did not fail")
    templates = uws_factory.create_templates()
    return templates.error(request, job.error)


@uws_router.get(
    "/{job_id}/executionduration",
    response_class=PlainTextResponse,
    summary="Execution duration of job",
)
async def get_job_execution_duration(
    job_id: str,
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(user, job_id)
    return str(job.execution_duration)


@uws_router.post(
    "/{job_id}/executionduration",
    response_class=RedirectResponse,
    status_code=303,
    summary="Change job execution duration",
)
async def post_job_execution_duration(
    job_id: str,
    request: Request,
    executionduration: Optional[int] = Form(
        None,
        title="New execution duration",
        description="Integer seconds of wall clock time.",
        example=14400,
    ),
    params: List[JobParameter] = Depends(uws_post_params_dependency),
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    # Work around the obnoxious requirement for case-insensitive parameters.
    for param in params:
        if param.parameter_id != "executionduration":
            msg = f"Unknown parameter {param.parameter_id}={param.value}"
            raise ParameterError(msg)
        try:
            executionduration = int(param.value)
        except Exception:
            raise ParameterError(f"Invalid duration {param.value}")
        if executionduration <= 0:
            raise ParameterError(f"Invalid duration {param.value}")
    if not executionduration:
        raise ParameterError("No new execution duration given")

    # Update the execution duration.  Note that the policy layer may modify
    # the execution duration, so the duration set may not match the input.
    job_service = uws_factory.create_job_service()
    await job_service.update_execution_duration(
        user, job_id, executionduration
    )
    return request.url_for("get_job", job_id=job_id)


@uws_router.get(
    "/{job_id}/owner",
    response_class=PlainTextResponse,
    summary="Owner of job",
)
async def get_job_owner(
    job_id: str,
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(user, job_id)
    return job.owner


@uws_router.get(
    "/{job_id}/parameters",
    responses={200: {"content": {"application/xml": {}}}},
    summary="Job parameters",
)
async def get_job_parameters(
    job_id: str,
    request: Request,
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> Response:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(user, job_id)
    templates = uws_factory.create_templates()
    return templates.parameters(request, job)


@uws_router.get(
    "/{job_id}/phase",
    response_class=PlainTextResponse,
    summary="Phase of job",
)
async def get_job_phase(
    job_id: str,
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(user, job_id)
    return job.phase.value


@uws_router.post(
    "/{job_id}/phase",
    response_class=RedirectResponse,
    status_code=303,
    summary="Start or abort job",
)
async def post_job_phase(
    job_id: str,
    request: Request,
    phase: Optional[Literal["RUN", "ABORT"]] = Form(
        None,
        title="Job state change",
        summary="RUN to start the job, ABORT to abort the job.",
    ),
    params: List[JobParameter] = Depends(uws_post_params_dependency),
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    # Work around the obnoxious requirement for case-insensitive parameters.
    for param in params:
        if param.parameter_id != "phase":
            msg = f"Unknown parameter {param.parameter_id}={param.value}"
            raise ParameterError(msg)
        if param.value not in ("RUN", "ABORT"):
            raise ParameterError(f"Invalid phase {param.value}")
        phase = param.value  # type: ignore[assignment]
    if not phase:
        raise ParameterError("No new phase given")

    # Dramatiq doesn't support aborting jobs, so currently neither do we.
    if phase == "ABORT":
        raise PermissionDeniedError("Aborting jobs is not supported")

    # The only remaining case is starting the job.
    job_service = uws_factory.create_job_service()
    await job_service.start(user, job_id)
    return request.url_for("get_job", job_id=job_id)


@uws_router.get(
    "/{job_id}/quote",
    response_class=PlainTextResponse,
    summary="Quote for job",
)
async def get_job_quote(
    job_id: str,
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> str:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(user, job_id)
    if job.quote:
        return isodatetime(job.quote)
    else:
        # The UWS standard says to return an empty text/plain response in this
        # case, as weird as it might look.
        return ""


@uws_router.get(
    "/{job_id}/results",
    responses={200: {"content": {"application/xml": {}}}},
    summary="Job results",
)
async def get_job_results(
    job_id: str,
    request: Request,
    user: str = Depends(auth_dependency),
    uws_factory: UWSFactory = Depends(uws_dependency),
) -> Response:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(user, job_id)
    templates = uws_factory.create_templates()
    return await templates.results(request, job)
