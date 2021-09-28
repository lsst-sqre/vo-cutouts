"""FastAPI dependencies for the UWS service."""

from typing import AsyncIterator, List, Optional

from dramatiq import Actor
from fastapi import Depends, Request
from safir.dependencies.logger import logger_dependency
from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger

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
    "uws_params_dependency",
]


class UWSFactory:
    """Build UWS components."""

    def __init__(
        self,
        *,
        config: UWSConfig,
        actor: Actor,
        policy: UWSPolicy,
        session: async_scoped_session,
        logger: BoundLogger,
    ) -> None:
        self._config = config
        self._actor = actor
        self._policy = policy
        self._session = session
        self._logger = logger

    def create_job_service(self) -> JobService:
        """Create a new UWS job metadata service."""
        storage = FrontendJobStore(self._session)
        return JobService(
            config=self._config,
            actor=self._actor,
            policy=self._policy,
            storage=storage,
        )

    def create_templates(self) -> UWSTemplates:
        """Create a new XML renderer for responses."""
        return UWSTemplates()


class UWSDependency:
    """Provides a UWS factory as a dependency."""

    def __init__(self) -> None:
        self._config: Optional[UWSConfig] = None
        self._actor: Optional[Actor] = None
        self._policy: Optional[UWSPolicy] = None
        self._session: Optional[async_scoped_session] = None

    async def __call__(
        self, logger: BoundLogger = Depends(logger_dependency)
    ) -> AsyncIterator[UWSFactory]:
        assert self._config, "UWSDependency not initialized"
        assert self._actor, "UWSDependency not initialized"
        assert self._policy, "UWSDependency not initialized"
        assert self._session, "UWSDependency not initialized"
        factory = UWSFactory(
            config=self._config,
            actor=self._actor,
            policy=self._policy,
            session=self._session,
            logger=logger,
        )
        yield factory
        await self._session.remove()

    async def initialize(
        self,
        *,
        config: UWSConfig,
        actor: Actor,
        policy: UWSPolicy,
        logger: BoundLogger,
        reset_database: bool = False,
    ) -> None:
        """Initialize the UWS subsystem.

        Parameters
        ----------
        config : `vocutouts.uws.config.UWSConfig`
            The UWS configuration.
        actor : `dramatiq.Actor`
            The backend task for this service.
        policy : `vocutouts.uws.policy.UWSPolicy`
            The UWS policy layer.
        logger : `structlog.stdlib.BoundLogger`
            Logger to use.
        reset_database : `bool`
            If set to `True`, drop all tables and reprovision the database.
            Useful when running tests with an external database.  Default is
            `False`.
        """
        self._config = config
        self._actor = actor
        self._policy = policy
        self._session = await create_async_session(self._config, logger)
        await initialize_database(config, logger, reset=reset_database)

    def override_actor(self, actor: Actor) -> None:
        """Change the actor used in subsequent invocations.

        This method is probably only useful for the test suite.

        Parameters
        ----------
        actor : `dramatiq.Actor`
            The new actor.
        """
        self._actor = actor

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


async def uws_params_dependency(request: Request) -> List[JobParameter]:
    """Parse parameters.

    The UWS standard has the annoying property that parameters may be provided
    via either the query or a POST body, and the keys for both are
    case-insensitive.  Modern web frameworks don't handle this easily.  This
    dependency therefore parses the queries and (in the case of a POST) the
    POST body directly from the Request, converts them to JobParameters, and
    returns those as a list.
    """
    params = [
        JobParameter(parameter_id=k.lower(), value=v, is_post=False)
        for k, v in request.query_params.items()
    ]
    if request.method == "POST":
        params.extend(
            [
                JobParameter(parameter_id=k.lower(), value=v, is_post=True)
                for k, v in (await request.form()).items()
            ]
        )
    return params
