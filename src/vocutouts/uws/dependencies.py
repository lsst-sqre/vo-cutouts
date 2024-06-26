"""FastAPI dependencies for the UWS service.

The UWS FastAPI support is initialized by the parent application via this
dependency's ``initialize`` method.  It then returns a `UWSFactory` on
request to individual route handlers, which in turn can create other needed
objects.
"""

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Form, Request
from safir.arq import ArqMode, ArqQueue, MockArqQueue, RedisArqQueue
from safir.database import create_async_session, create_database_engine
from safir.dependencies.logger import logger_dependency
from sqlalchemy.ext.asyncio import AsyncEngine, async_scoped_session
from structlog.stdlib import BoundLogger

from .config import UWSConfig
from .models import UWSJobParameter
from .responses import UWSTemplates
from .results import ResultStore
from .service import JobService
from .storage import JobStore

__all__ = [
    "UWSDependency",
    "UWSFactory",
    "runid_post_dependency",
    "uws_dependency",
    "uws_post_params_dependency",
]


class UWSFactory:
    """Build UWS components.

    Parameters
    ----------
    config
        UWS configuration.
    arq
        arq queue to use.
    session
        Database session.
    result_store
        Signed URL generator for results.
    logger
        Logger to use.

    Attributes
    ----------
    session
        Database session. This is exposed primarily for the test suite. It
        shouldn't be necessary for other code to use it directly.
    """

    def __init__(
        self,
        *,
        config: UWSConfig,
        arq: ArqQueue,
        session: async_scoped_session,
        result_store: ResultStore,
        logger: BoundLogger,
    ) -> None:
        self.session = session
        self._config = config
        self._arq = arq
        self._result_store = result_store
        self._logger = logger

    def create_result_store(self) -> ResultStore:
        """Return a wrapper around the result storage."""
        return self._result_store

    def create_job_service(self) -> JobService:
        """Create a new UWS job metadata service."""
        return JobService(
            config=self._config,
            arq_queue=self._arq,
            storage=self.create_job_store(),
            result_store=self._result_store,
            logger=self._logger,
        )

    def create_job_store(self) -> JobStore:
        """Create a new UWS job store."""
        return JobStore(self.session)

    def create_templates(self) -> UWSTemplates:
        """Create a new XML renderer for responses."""
        return UWSTemplates(self._result_store)


class UWSDependency:
    """Initializes UWS and provides a UWS factory as a dependency."""

    def __init__(self) -> None:
        self._arq: ArqQueue
        self._config: UWSConfig
        self._engine: AsyncEngine
        self._session: async_scoped_session
        self._result_store: ResultStore

    async def __call__(
        self, logger: Annotated[BoundLogger, Depends(logger_dependency)]
    ) -> AsyncIterator[UWSFactory]:
        try:
            yield UWSFactory(
                config=self._config,
                arq=self._arq,
                session=self._session,
                result_store=self._result_store,
                logger=logger,
            )
        finally:
            # Following the recommendations in the SQLAlchemy documentation,
            # each session is scoped to a single web request. However, this
            # all uses the same async_scoped_session object, so should share
            # an underlying engine and connection pool.
            await self._session.remove()

    async def aclose(self) -> None:
        """Shut down the UWS subsystem."""
        await self._engine.dispose()

    async def initialize(self, config: UWSConfig) -> None:
        """Initialize the UWS subsystem.

        Parameters
        ----------
        config
            UWS configuration.
        """
        self._config = config
        self._result_store = ResultStore(config)
        if config.arq_mode == ArqMode.production:
            settings = config.arq_redis_settings
            self._arq = await RedisArqQueue.initialize(settings)
        else:
            self._arq = MockArqQueue()
        self._engine = create_database_engine(
            config.database_url,
            config.database_password,
            isolation_level="REPEATABLE READ",
        )
        self._session = await create_async_session(self._engine)

    def override_arq_queue(self, arq_queue: ArqQueue) -> None:
        """Change the arq used in subsequent invocations.

        This method is probably only useful for the test suite.

        Parameters
        ----------
        arq
            New arq queue.
        """
        self._arq = arq_queue


uws_dependency = UWSDependency()


async def uws_post_params_dependency(
    request: Request,
) -> list[UWSJobParameter]:
    """Parse POST parameters.

    UWS requires that all POST parameters be case-insensitive, which is not
    supported by FastAPI or Starlette.  POST parameters therefore have to be
    parsed by this dependency and then extracted from the resulting
    `~vocutouts.uws.models.UWSJobParameter` list (which unfortunately also
    means revalidating their types).

    The POST parameters can also be (and should be) listed independently as
    dependencies using the normal FastAPI syntax, in order to populate the
    OpenAPI schema, but unfortunately they all have to be listed as optional
    from FastAPI's perspective because they may be present using different
    capitalization.
    """
    if request.method != "POST":
        raise ValueError("uws_post_params_dependency used for non-POST route")
    parameters = []
    for key, value in (await request.form()).items():
        if not isinstance(value, str):
            raise TypeError("File upload not supported")
        parameters.append(
            UWSJobParameter(
                parameter_id=key.lower(), value=value, is_post=True
            )
        )
    return parameters


async def runid_post_dependency(
    *,
    runid: Annotated[
        str | None,
        Form(
            title="Run ID for job",
            description=(
                "An opaque string that is returned in the job metadata and"
                " job listings. May be used by the client to associate jobs"
                " with specific larger operations."
            ),
        ),
    ] = None,
    params: Annotated[
        list[UWSJobParameter], Depends(uws_post_params_dependency)
    ],
) -> str | None:
    """Parse the run ID from POST parameters.

    This is annoyingly complex because DALI defines all parameters as
    case-insensitive, so we have to list the field as a dependency but then
    parse it out of the case-canonicalized job parameters.
    """
    for param in params:
        if param.parameter_id == "runid":
            runid = param.value
    return runid
