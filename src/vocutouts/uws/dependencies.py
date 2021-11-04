"""FastAPI dependencies for the UWS service.

The UWS FastAPI support is initialized by the parent application via this
dependency's ``initialize`` method.  It then returns a `UWSFactory` on
request to individual route handlers, which in turn can create other needed
objects.
"""

from typing import AsyncIterator, List, Optional

from fastapi import Depends, Request
from safir.dependencies.logger import logger_dependency
from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger

from .butler import UWSButler
from .config import UWSConfig
from .database import create_async_session, initialize_database
from .models import JobParameter
from .policy import UWSPolicy
from .responses import UWSTemplates
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
        butler: UWSButler,
        logger: BoundLogger,
    ) -> None:
        self._config = config
        self._policy = policy
        self._session = session
        self._butler = butler
        self._logger = logger

    def create_butler(self) -> UWSButler:
        """Return a wrapper around Butler."""
        return self._butler

    def create_job_service(self) -> JobService:
        """Create a new UWS job metadata service."""
        storage = FrontendJobStore(self._session)
        return JobService(
            config=self._config, policy=self._policy, storage=storage
        )

    def create_templates(self) -> UWSTemplates:
        """Create a new XML renderer for responses."""
        return UWSTemplates(self._butler)


class UWSDependency:
    """Initializes UWS and provides a UWS factory as a dependency."""

    def __init__(self) -> None:
        self._config: Optional[UWSConfig] = None
        self._policy: Optional[UWSPolicy] = None
        self._session: Optional[async_scoped_session] = None
        self._butler: Optional[UWSButler] = None

    async def __call__(
        self, logger: BoundLogger = Depends(logger_dependency)
    ) -> AsyncIterator[UWSFactory]:
        # Tell mypy that not calling initialize first is an error.  This would
        # fail anyway without the asserts when something tried to use the None
        # value.
        assert self._config, "UWSDependency not initialized"
        assert self._policy, "UWSDependency not initialized"
        assert self._session, "UWSDependency not initialized"
        assert self._butler, "UWSDependency not initialized"
        factory = UWSFactory(
            config=self._config,
            policy=self._policy,
            session=self._session,
            butler=self._butler,
            logger=logger,
        )
        yield factory

        # Following the recommendations in the SQLAlchemy documentation, each
        # session is scoped to a single web request.  However, this all uses
        # the same async_scoped_session object, so should share an underlying
        # engine and connection pool.
        await self._session.remove()

    async def initialize(
        self,
        *,
        config: UWSConfig,
        policy: UWSPolicy,
        logger: BoundLogger,
        reset_database: bool = False,
    ) -> None:
        """Initialize the UWS subsystem.

        Parameters
        ----------
        config : `vocutouts.uws.config.UWSConfig`
            The UWS configuration.
        policy : `vocutouts.uws.policy.UWSPolicy`
            The UWS policy layer.
        logger : `structlog.stdlib.BoundLogger`
            Logger to use during database initialization.  This is not saved;
            subsequent invocations as a dependency will create a new logger
            from the triggering request.
        reset_database : `bool`
            If set to `True`, drop all tables and reprovision the database.
            Useful when running the test suite with an external database.
            Default is `False`.
        """
        self._config = config
        self._policy = policy
        self._session = await create_async_session(config, logger)
        await initialize_database(config, logger, reset=reset_database)
        self._butler = UWSButler(config.butler_repository, config)

    def override_policy(self, policy: UWSPolicy) -> None:
        """Change the actor used in subsequent invocations.

        This method is probably only useful for the test suite.

        Parameters
        ----------
        actor : `vocutouts.uws.policy.UWSPolicy`
            The new policy.
        """
        self._policy = policy


uws_dependency = UWSDependency()


async def uws_post_params_dependency(request: Request) -> List[JobParameter]:
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
    return [
        JobParameter(parameter_id=k.lower(), value=v, is_post=True)
        for k, v in (await request.form()).items()
    ]
