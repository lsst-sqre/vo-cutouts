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
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Form, Query, Request, Response
from fastapi.responses import PlainTextResponse, RedirectResponse
from safir.datetime import isodatetime, parse_isodatetime
from safir.dependencies.gafaelfawr import (
    auth_delegated_token_dependency,
    auth_dependency,
    auth_logger_dependency,
)
from structlog.stdlib import BoundLogger

from .dependencies import (
    UWSFactory,
    uws_dependency,
    uws_post_params_dependency,
)
from .exceptions import DataMissingError, ParameterError, PermissionDeniedError
from .models import ExecutionPhase, UWSJobParameter

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
    *,
    request: Request,
    phase: Annotated[
        list[ExecutionPhase] | None,
        Query(
            title="Execution phase",
            description="Limit results to the provided execution phases",
        ),
    ] = None,
    after: Annotated[
        datetime | None,
        Query(
            title="Creation date",
            description="Limit results to jobs created after this date",
        ),
    ] = None,
    last: Annotated[
        int | None,
        Query(
            title="Number of jobs",
            description="Return at most the given number of jobs",
        ),
    ] = None,
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
) -> Response:
    job_service = uws_factory.create_job_service()
    jobs = await job_service.list_jobs(
        user, phases=phase, after=after, count=last
    )
    base_url = request.url_for("get_job_list")
    templates = uws_factory.create_templates()
    return templates.job_list(request, jobs, str(base_url))


@uws_router.get(
    "/{job_id}",
    responses={200: {"content": {"application/xml": {}}}},
    summary="Job details",
)
async def get_job(
    *,
    job_id: str,
    request: Request,
    wait: Annotated[
        int | None,
        Query(
            title="Wait for status changes",
            description=(
                "Maximum number of seconds to wait or -1 to wait for as long"
                " as the server permits"
            ),
        ),
    ] = None,
    phase: Annotated[
        ExecutionPhase | None,
        Query(
            title="Initial phase for waiting",
            description=(
                "When waiting for status changes, consider this to be the"
                " initial execution phase. If the phase has already changed,"
                " return immediately. This parameter should always be"
                " provided when wait is used."
            ),
        ),
    ] = None,
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
) -> Response:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(
        user, job_id, wait_seconds=wait, wait_phase=phase
    )
    templates = uws_factory.create_templates()
    return await templates.job(request, job)


@uws_router.delete(
    "/{job_id}",
    status_code=303,
    response_class=RedirectResponse,
    summary="Delete a job",
)
async def delete_job(
    *,
    job_id: str,
    request: Request,
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
    logger: Annotated[BoundLogger, Depends(auth_logger_dependency)],
) -> str:
    job_service = uws_factory.create_job_service()
    await job_service.delete(user, job_id)
    logger.info("Deleted job", job_id=job_id)
    return str(request.url_for("get_job_list"))


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
    *,
    job_id: str,
    request: Request,
    action: Annotated[
        Literal["DELETE"] | None,
        Form(
            title="Action to perform",
            description="Mandatory, must be set to DELETE",
        ),
    ] = None,
    params: Annotated[
        list[UWSJobParameter], Depends(uws_post_params_dependency)
    ],
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
    logger: Annotated[BoundLogger, Depends(auth_logger_dependency)],
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
    logger.info("Deleted job", job_id=job_id)
    return str(request.url_for("get_job_list"))


@uws_router.get(
    "/{job_id}/destruction",
    response_class=PlainTextResponse,
    summary="Destruction time for job",
)
async def get_job_destruction(
    job_id: str,
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
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
    *,
    job_id: str,
    request: Request,
    destruction: Annotated[
        datetime | None,
        Form(
            title="New destruction time",
            description="Must be in ISO 8601 format.",
            examples=["2021-09-10T10:01:02Z"],
        ),
    ] = None,
    params: Annotated[
        list[UWSJobParameter], Depends(uws_post_params_dependency)
    ],
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
    logger: Annotated[BoundLogger, Depends(auth_logger_dependency)],
) -> str:
    # Work around the obnoxious requirement for case-insensitive parameters.
    for param in params:
        if param.parameter_id != "destruction":
            msg = f"Unknown parameter {param.parameter_id}={param.value}"
            raise ParameterError(msg)
        try:
            destruction = parse_isodatetime(param.value)
        except Exception as e:
            raise ParameterError(f"Invalid date {param.value}") from e
    if not destruction:
        raise ParameterError("No new destruction time given")

    # Update the destruction time.  Note that the policy layer may modify the
    # destruction time, so the time set may not match the input.
    # update_destruction returns the actual time set, or None if the time was
    # not changed.
    job_service = uws_factory.create_job_service()
    new_destruction = await job_service.update_destruction(
        user, job_id, destruction
    )
    if new_destruction:
        logger.info(
            "Changed job destruction time",
            job_id=job_id,
            destruction=isodatetime(new_destruction),
        )
    return str(request.url_for("get_job", job_id=job_id))


@uws_router.get(
    "/{job_id}/error",
    responses={
        200: {"content": {"application/xml": {}}},
        404: {"description": "Job not found or job did not fail"},
    },
    summary="Job error",
)
async def get_job_error(
    *,
    job_id: str,
    request: Request,
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
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
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
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
    *,
    job_id: str,
    request: Request,
    executionduration: Annotated[
        int | None,
        Form(
            title="New execution duration",
            description="Integer seconds of wall clock time.",
            examples=[14400],
        ),
    ] = None,
    params: Annotated[
        list[UWSJobParameter], Depends(uws_post_params_dependency)
    ],
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
    logger: Annotated[BoundLogger, Depends(auth_logger_dependency)],
) -> str:
    # Work around the obnoxious requirement for case-insensitive parameters.
    for param in params:
        if param.parameter_id != "executionduration":
            msg = f"Unknown parameter {param.parameter_id}={param.value}"
            raise ParameterError(msg)
        try:
            executionduration = int(param.value)
        except Exception as e:
            raise ParameterError(f"Invalid duration {param.value}") from e
        if executionduration <= 0:
            raise ParameterError(f"Invalid duration {param.value}")
    if not executionduration:
        raise ParameterError("No new execution duration given")

    # Update the execution duration.  Note that the policy layer may modify
    # the execution duration, so the duration set may not match the input.
    # update_execution_duration returns the new execution duration set, or
    # None if it was not changed.
    job_service = uws_factory.create_job_service()
    new_executionduration = await job_service.update_execution_duration(
        user, job_id, executionduration
    )
    if new_executionduration is not None:
        logger.info(
            "Changed job execution duration",
            job_id=job_id,
            duration=f"{new_executionduration}s",
        )
    return str(request.url_for("get_job", job_id=job_id))


@uws_router.get(
    "/{job_id}/owner",
    response_class=PlainTextResponse,
    summary="Owner of job",
)
async def get_job_owner(
    job_id: str,
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
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
    *,
    job_id: str,
    request: Request,
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
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
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
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
    *,
    job_id: str,
    request: Request,
    phase: Annotated[
        Literal["RUN", "ABORT"] | None,
        Form(
            title="Job state change",
            summary="RUN to start the job, ABORT to abort the job.",
        ),
    ] = None,
    params: Annotated[
        list[UWSJobParameter], Depends(uws_post_params_dependency)
    ],
    user: Annotated[str, Depends(auth_dependency)],
    access_token: Annotated[str, Depends(auth_delegated_token_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
    logger: Annotated[BoundLogger, Depends(auth_logger_dependency)],
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
    metadata = await job_service.start(user, job_id, access_token)
    logger.info("Started job", job_id=job_id, arq_job_id=metadata.id)
    return str(request.url_for("get_job", job_id=job_id))


@uws_router.get(
    "/{job_id}/quote",
    response_class=PlainTextResponse,
    summary="Quote for job",
)
async def get_job_quote(
    job_id: str,
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
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
    *,
    job_id: str,
    request: Request,
    user: Annotated[str, Depends(auth_dependency)],
    uws_factory: Annotated[UWSFactory, Depends(uws_dependency)],
) -> Response:
    job_service = uws_factory.create_job_service()
    job = await job_service.get(user, job_id)
    templates = uws_factory.create_templates()
    return await templates.results(request, job)
