"""Representation of a UWS application."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse
from safir.database import create_database_engine, initialize_database
from structlog.stdlib import BoundLogger

from . import schema
from .config import UWSConfig
from .constants import UWS_QUEUE_NAME
from .exceptions import UWSError
from .handlers import (
    install_async_post_handler,
    install_sync_get_handler,
    install_sync_post_handler,
    uws_router,
)
from .uwsworker import WorkerSettings
from .workers import (
    close_uws_worker_context,
    create_uws_worker_context,
    uws_job_completed,
    uws_job_started,
)

__all__ = ["UWSApplication"]


async def _uws_error_handler(
    request: Request, exc: UWSError
) -> PlainTextResponse:
    response = f"{exc.error_code.value} {exc!s}\n"
    if exc.detail:
        response += "\n{exc.detail}"
    return PlainTextResponse(response, status_code=exc.status_code)


async def _usage_handler(
    request: Request, exc: RequestValidationError
) -> PlainTextResponse:
    return PlainTextResponse(f"UsageError\n\n{exc!s}", status_code=422)


class UWSApplication:
    """Glue between a FastAPI application and the UWS implementation.

    An instance of this class should be created during construction of the
    service that will use the UWS layer. It provides methods to initialize the
    UWS database, build route handlers, install error handlers, and build the
    UWS database worker. Construction of the backend worker that does the work
    of the service is handled separately so that it can have minimal
    dependencies.

    Parameters
    ----------
    config
        UWS configuration.
    """

    def __init__(self, config: UWSConfig) -> None:
        self._config = config

    def build_worker(self, logger: BoundLogger) -> WorkerSettings:
        """Construct an arq worker configuration for the UWS worker.

        All UWS job status and results must be stored in the underlying
        database, since the API serves job information from there. To minimize
        dependencies for the worker, which may (for example) pin its own
        version of SQLAlchemy that may not be compatible with that used by the
        application, the actual worker is not responsible for storing the
        results in SQL. Instead, it returns results via arq, which temporarily
        puts them in Redis then uses ``on_job_start`` and ``after_job_end`` to
        notify a different queue. Those results are recovered and stored in
        the database by separate a separate arq worker.

        This function returns a class suitable for assigning to a module
        variable and referencing as the argument to the :command:`arq`
        command-line tool to start the worker.

        Parameters
        ----------
        logger
            Logger to use for messages.
        """

        async def startup(ctx: dict[Any, Any]) -> None:
            ctx.update(await create_uws_worker_context(self._config, logger))

        async def shutdown(ctx: dict[Any, Any]) -> None:
            await close_uws_worker_context(ctx)

        return WorkerSettings(
            functions=[uws_job_started, uws_job_completed],
            redis_settings=self._config.arq_redis_settings,
            job_timeout=timedelta(seconds=30),
            queue_name=UWS_QUEUE_NAME,
            on_startup=startup,
            on_shutdown=shutdown,
        )

    async def initialize_uws_database(
        self, logger: BoundLogger, *, reset: bool = False
    ) -> None:
        """Initialize the UWS database.

        Parameters
        ----------
        logger
            Logger to use.
        reset
            If `True`, also delete all data in the database.
        """
        engine = create_database_engine(
            self._config.database_url, self._config.database_password
        )
        await initialize_database(
            engine, logger, schema=schema.Base.metadata, reset=reset
        )
        await engine.dispose()

    def install_error_handlers(self, app: FastAPI) -> None:
        """Install error handlers that follow DALI and UWS conventions.

        This method must be called during application setup for any FastAPI
        app using the UWS layer for correct error message handling. This will
        change the error response for all parameter validation errors from
        FastAPI.

        Currently these error handlers return ``text/plain`` errors. VOTable
        errors may be a better choice, but revision 1.0 of the SODA standard
        only allows ``text/plain`` errors for sync routes.
        """
        app.exception_handler(UWSError)(_uws_error_handler)
        app.exception_handler(RequestValidationError)(_usage_handler)

    def install_handlers(self, router: APIRouter) -> None:
        """Install the route handlers for the service.

        This method will always install a POST handler at the root of the
        router that creates an async job, and handlers under ``/jobs`` that
        implement the UWS protocol for managing those jobs. If
        ``sync_post_dependency`` is set in the
        `~vocutouts.uws.interface.UWSInterface` that this application was
        configured with, a POST handler for ``/sync`` to create a sync job
        will be added. If ``sync_get_dependency`` is set, a GET handler for
        ``/sync`` to create a sync job will be added.
        """
        router.include_router(uws_router, prefix="/jobs")
        if dependency := self._config.sync_get_dependency:
            install_sync_get_handler(router, dependency)
        if dependency := self._config.sync_post_dependency:
            install_sync_post_handler(router, dependency)
        dependency = self._config.async_post_dependency
        install_async_post_handler(router, dependency)
