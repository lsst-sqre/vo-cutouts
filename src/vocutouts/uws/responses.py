"""Return internal objects as XML responses."""

from __future__ import annotations

from pathlib import Path

from fastapi import Request, Response
from fastapi.templating import Jinja2Templates

from .models import Availability, Job, JobDescription, JobError
from .results import ResultStore
from .utils import isodatetime

__all__ = ["UWSTemplates"]


_templates = Jinja2Templates(
    directory=str(Path(__file__).parent / "templates")
)
_templates.env.filters["isodatetime"] = isodatetime


class UWSTemplates:
    """Template responses for the UWS protocol.

    This also includes VOSI-Availability since it was convenient to provide.
    """

    def __init__(self, result_store: ResultStore) -> None:
        self._result_store = result_store

    def availability(
        self, request: Request, availability: Availability
    ) -> Response:
        """Return the availability of a service as an XML response."""
        return _templates.TemplateResponse(
            "availability.xml",
            {"availability": availability, "request": request},
            media_type="application/xml",
        )

    def error(self, request: Request, error: JobError) -> Response:
        """Return the error of a job as an XML response."""
        return _templates.TemplateResponse(
            "error.xml",
            {"error": error, "request": request},
            media_type="application/xml",
        )

    async def job(self, request: Request, job: Job) -> Response:
        """Return a job as an XML response."""
        results = [
            await self._result_store.url_for_result(r) for r in job.results
        ]
        return _templates.TemplateResponse(
            "job.xml",
            {"job": job, "results": results, "request": request},
            media_type="application/xml",
        )

    def job_list(
        self, request: Request, jobs: list[JobDescription], base_url: str
    ) -> Response:
        """Return a list of jobs as an XML response."""
        return _templates.TemplateResponse(
            "jobs.xml",
            {"base_url": base_url, "jobs": jobs, "request": request},
            media_type="application/xml",
        )

    def parameters(self, request: Request, job: Job) -> Response:
        """Return the parameters for a job as an XML response."""
        return _templates.TemplateResponse(
            "parameters.xml",
            {"job": job, "request": request},
            media_type="application/xml",
        )

    async def results(self, request: Request, job: Job) -> Response:
        """Return the results for a job as an XML response."""
        results = [
            await self._result_store.url_for_result(r) for r in job.results
        ]
        return _templates.TemplateResponse(
            "results.xml",
            {"results": results, "request": request},
            media_type="application/xml",
        )
