"""The global broker used by the image cutout service.

Loading this module defines a default Dramatiq broker used by both the API
frontend and the UWS database worker.  This module is **not** used by the
cutout implementation backend, which is a standalone Python module that
creates its own (compatible) broker.

This module must be loaded before any code using @dramatiq.actor is imported,
or those tasks will be associated with a RabbitMQ broker.
"""

from __future__ import annotations

from typing import Optional

import dramatiq
import structlog
from dramatiq import Broker, Middleware, Worker
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import CurrentMessage
from dramatiq.results import Results
from dramatiq.results.backends import RedisBackend
from safir.database import create_sync_session
from sqlalchemy import select
from sqlalchemy.orm import scoped_session

from .config import config
from .uws.schema import Job

broker = RedisBroker(host=config.redis_host, password=config.redis_password)
"""Broker used by UWS."""

results = RedisBackend(host=config.redis_host, password=config.redis_password)
"""Result backend used by UWS."""

worker_session: Optional[scoped_session] = None
"""Shared scoped session used by the UWS worker."""


class WorkerSession(Middleware):
    """Middleware to create a SQLAlchemy scoped session for a worker."""

    def before_worker_boot(self, broker: Broker, worker: Worker) -> None:
        """Initialize the database session before worker threads start.

        This is run in the main process by the ``dramatiq`` CLI before
        starting the worker threads, so it should run in a single-threaded
        context.
        """
        global worker_session
        if worker_session is None:
            logger = structlog.get_logger(config.logger_name)
            worker_session = create_sync_session(
                config.database_url,
                config.database_password,
                logger,
                isolation_level="REPEATABLE READ",
                statement=select(Job.id),
            )


# This must be done as early as possible so that actors are registered with
# the correct broker and the middleware is set up before the actors are
# registered.  When the UWS worker is started with dramatiq, the FastAPI app
# won't be created, so we can't do this from the app lifecycle hook.
#
# Do NOT load the Callbacks middleware here, even though it is used.  It is
# loaded by default, and loading it again results in duplicate messages to the
# UWS database workers.
dramatiq.set_broker(broker)
broker.add_middleware(CurrentMessage())
broker.add_middleware(Results(backend=results))
broker.add_middleware(WorkerSession())
