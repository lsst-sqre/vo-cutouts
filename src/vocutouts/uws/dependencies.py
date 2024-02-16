"""FastAPI dependencies for the UWS service.

The UWS FastAPI support is initialized by the parent application via this
dependency's ``initialize`` method.  It then returns a `UWSFactory` on
request to individual route handlers, which in turn can create other needed
objects.
"""

from typing import Annotated

from fastapi import Depends, Request
from safir.dependencies.db_session import db_session_dependency
from safir.dependencies.logger import logger_dependency
from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger

from .config import UWSConfig
from .models import JobParameter
from .policy import UWSPolicy
from .responses import UWSTemplates
from .results import ResultStore
from .service import JobService
from .storage import FrontendJobStore

__all__ = [
    "UWSDependency",
    "UWSFactory",
    "uws_dependency",
    "uws_post_params_dependency",
]


class UWSFactory:
    """Build UWS components."""

    def __init__(
        self,
        *,
        config: UWSConfig,
        policy: UWSPolicy,
        session: async_scoped_session,
        result_store: ResultStore,
        logger: BoundLogger,
    ) -> None:
        self._config = config
        self._policy = policy
        self._session = session
        self._result_store = result_store
        self._logger = logger

    def create_result_store(self) -> ResultStore:
        """Return a wrapper around the result storage."""
        return self._result_store

    def create_job_service(self) -> JobService:
        """Create a new UWS job metadata service."""
        storage = FrontendJobStore(self._session)
        return JobService(
            config=self._config, policy=self._policy, storage=storage
        )

    def create_templates(self) -> UWSTemplates:
        """Create a new XML renderer for responses."""
        return UWSTemplates(self._result_store)


class UWSDependency:
    """Initializes UWS and provides a UWS factory as a dependency."""

    def __init__(self) -> None:
        self._config: UWSConfig | None = None
        self._policy: UWSPolicy | None = None
        self._result_store: ResultStore | None = None

    async def __call__(
        self,
        session: Annotated[
            async_scoped_session, Depends(db_session_dependency)
        ],
        logger: Annotated[BoundLogger, Depends(logger_dependency)],
    ) -> UWSFactory:
        # Tell mypy that not calling initialize first is an error.  This would
        # fail anyway without the asserts when something tried to use the None
        # value.
        if not (self._config and self._policy and self._result_store):
            raise RuntimeError("UWSDependency not initialized")
        return UWSFactory(
            config=self._config,
            policy=self._policy,
            session=session,
            result_store=self._result_store,
            logger=logger,
        )

    async def aclose(self) -> None:
        """Shut down the UWS subsystem."""
        await db_session_dependency.aclose()

    async def initialize(
        self,
        *,
        config: UWSConfig,
        policy: UWSPolicy,
        logger: BoundLogger,
    ) -> None:
        """Initialize the UWS subsystem.

        Parameters
        ----------
        config
            The UWS configuration.
        policy
            The UWS policy layer.
        logger
            Logger to use during database initialization.  This is not saved;
            subsequent invocations as a dependency will create a new logger
            from the triggering request.
        """
        self._config = config
        self._policy = policy
        self._result_store = ResultStore(config)
        await db_session_dependency.initialize(
            config.database_url,
            config.database_password,
            isolation_level="REPEATABLE READ",
        )

    def override_policy(self, policy: UWSPolicy) -> None:
        """Change the actor used in subsequent invocations.

        This method is probably only useful for the test suite.

        Parameters
        ----------
        actor
            The new policy.
        """
        self._policy = policy


uws_dependency = UWSDependency()


async def uws_post_params_dependency(request: Request) -> list[JobParameter]:
    """Parse POST parameters.

    UWS requires that all POST parameters be case-insensitive, which is not
    supported by FastAPI or Starlette.  POST parameters therefore have to be
    parsed by this dependency and then extracted from the resulting
    `~vocutouts.uws.models.JobParameter` list (which unfortunately also means
    revalidating their types).

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
            JobParameter(parameter_id=key.lower(), value=value, is_post=True)
        )
    return parameters
