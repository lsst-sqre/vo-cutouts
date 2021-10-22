"""Utility functions for database management.

SQLAlchemy, when creating a database schema, can only know about the tables
that have been registered via a metaclass.  This module therefore must import
every schema to ensure that SQLAlchemy has a complete view.
"""

from __future__ import annotations

import time
from asyncio import current_task
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from sqlalchemy import create_engine, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_scoped_session,
    create_async_engine,
)
from sqlalchemy.orm import scoped_session, sessionmaker

from .schema import Job, drop_schema, initialize_schema

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine
    from structlog.stdlib import BoundLogger

    from .config import UWSConfig

__all__ = [
    "create_async_session",
    "create_sync_session",
    "initialize_database",
]


def _build_database_url(config: UWSConfig, *, is_async: bool) -> str:
    """Build the authenticated URL for the database.

    Parameters
    ----------
    config : `vocutouts.uws.UWSConfig`
        The UWS configuration.
    is_async : `bool`
        Whether the resulting URL should be async or not.

    Returns
    -------
    url : `str`
        The URL including the password.
    """
    database_url = config.database_url
    if is_async or config.database_password:
        parsed_url = urlparse(database_url)
        if is_async and parsed_url.scheme == "postgresql":
            parsed_url = parsed_url._replace(scheme="postgresql+asyncpg")
        if config.database_password:
            database_netloc = (
                f"{parsed_url.username}:{config.database_password}"
                f"@{parsed_url.hostname}"
            )
            parsed_url = parsed_url._replace(netloc=database_netloc)
        database_url = parsed_url.geturl()
    return database_url


def _create_async_scoped_session(engine: AsyncEngine) -> async_scoped_session:
    """Create a task-scoped asyncio session."""
    session_factory = sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )
    return async_scoped_session(session_factory, scopefunc=current_task)


async def create_async_session(
    config: UWSConfig, logger: BoundLogger
) -> async_scoped_session:
    """Create a new async database session.

    Checks that the database is available and retries in a loop for 10s if it
    is not.  The resulting session object is an asyncio scoped session, which
    means that it will materialize new AsyncSession objects for each asyncio
    task (and thus each web request).

    Parameters
    ----------
    config : `vocutouts.uws.UWSConfig`
        The UWS configuration.
    logger : `structlog.stdlib.BoundLogger`
        Logger to use.

    Returns
    -------
    session : `sqlalchemy.ext.asyncio.async_scoped_session`
        The database session proxy.
    """
    url = _build_database_url(config, is_async=True)
    for _ in range(5):
        try:
            engine = create_async_engine(url)
            session = _create_async_scoped_session(engine)
            async with session.begin():
                await session.execute(select(Job.id).limit(1))
                return session
        except OperationalError:
            logger.info("database not ready, waiting two seconds")
            time.sleep(2)
            continue

    # If we got here, we failed five times.  Try one last time without
    # catching exceptions so that we raise the appropriate exception to our
    # caller.
    engine = create_async_engine(url)
    session = _create_async_scoped_session(engine)
    async with session.begin():
        await session.execute(select(Job.id).limit(1))
        return session


def create_sync_session(
    config: UWSConfig, logger: BoundLogger
) -> scoped_session:
    """Create a new sync database session.

    This is used by the worker backend and thus doesn't need async support.
    It still needs scoped sessions because Dramatiq spawns multiple worker
    threads that each need to make database calls, but those sessions can live
    for the process lifetime.

    Parameters
    ----------
    config : `vocutouts.uws.UWSConfig`
        The UWS configuration.
    logger : `structlog.stdlib.BoundLogger`
        Logger to use.

    Returns
    -------
    session : `sqlalchemy.orm.scoped_session`
        The database session proxy.
    """
    url = _build_database_url(config, is_async=False)
    print(url)
    for _ in range(5):
        try:
            engine = create_engine(url)
            session_factory = sessionmaker(bind=engine)
            session = scoped_session(session_factory)
            with session.begin():
                session.execute(select(Job.id).limit(1))
                return session
        except OperationalError:
            logger.info("database not ready, waiting two seconds")
            time.sleep(2)
            continue

    # If we got here, we failed five times.  Try one last time without
    # catching exceptions so that we raise the appropriate exception to our
    # caller.
    engine = create_engine(url)
    session_factory = sessionmaker(bind=engine)
    session = scoped_session(session_factory)
    with session.begin():
        session.execute(select(Job.id).limit(1))
        return session


async def initialize_database(
    config: UWSConfig, logger: BoundLogger, reset: bool = False
) -> None:
    """Create and initialize a new database.

    Parameters
    ----------
    config : `vocutouts.uws.UWSConfig`
        The UWS configuration.
    logger : `structlog.stdlib.BoundLogger`
        Logger to use.
    reset : `bool`
        If set to `True`, drop all tables and reprovision the database.
        Useful when running tests with an external database.  Default is
        `False`.
    """
    url = _build_database_url(config, is_async=True)

    # Try up to five times to initialize the database schema.
    for _ in range(5):
        try:
            engine = create_async_engine(url)
            if reset:
                await drop_schema(engine)
            await initialize_schema(engine)
            success = True
        except OperationalError:
            logger.info("database not ready, waiting two seconds")
            time.sleep(2)
            continue
        if success:
            logger.info("initialized database schema")
            await engine.dispose()
            return
        break

    # If we got here, we failed five times to initialize the schema.
    msg = "database schema initialization failed (database not reachable?)"
    logger.error(msg)
    await engine.dispose()
