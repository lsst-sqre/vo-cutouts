"""nox configuration for vo-cutouts."""

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import nox
from nox_uv import session
from testcontainers.redis import RedisContainer

# Default sessions.
nox.options.sessions = ["lint", "typing", "test"]

# Other nox defaults.
nox.options.default_venv_backend = "uv"
nox.options.reuse_existing_virtualenvs = True

# Redis image to use for testing.
REDIS_IMAGE = "redis:8"


@dataclass
class _Containers:
    """Information about the started containers."""

    env: dict[str, str]
    """Additional Gafaelfawr environment variables to set."""

    redis: RedisContainer
    """testcontainers Redis container object."""


@contextmanager
def _start_containers() -> Iterator[_Containers]:
    """Start the containers needed to run tests.

    Yields
    ------
    dict of str
        Environment variables to set when running Alembic, required to load
        the Gafaelfawr configuration.
    """
    # testcontainers is annoyingly verbose by default because nox enables
    # debug logging.
    logging.getLogger("docker").setLevel(logging.INFO)
    logging.getLogger("testcontainers").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)

    # Start the containers and flesh out env with the additional required
    # Semaphore settings.
    with RedisContainer(
        image=REDIS_IMAGE, password=os.urandom(16).hex()
    ) as redis:
        redis_host = redis.get_container_host_ip()
        redis_port = redis.get_exposed_port(redis.port)
        redis_url = f"redis://{redis_host}:{redis_port}"
        yield _Containers(
            env={
                "CUTOUT_ARQ_QUEUE_PASSWORD": redis.password,
                "CUTOUT_ARQ_QUEUE_URL": f"{redis_url}/0",
            },
            redis=redis,
        )


@session(name="coverage-report", uv_groups=["dev"])
def coverage_report(session: nox.Session) -> None:
    """Generate a code coverage report from the test suite."""
    session.run("coverage", "report", *session.posargs)


@session(uv_only_groups=["lint"], uv_no_install_project=True)
def lint(session: nox.Session) -> None:
    """Run pre-commit hooks."""
    session.run("pre-commit", "run", "--all-files", *session.posargs)


@session(uv_groups=["dev"])
def test(session: nox.Session) -> None:
    """Test the Semaphore server."""
    with _start_containers() as containers:
        session.run(
            "pytest",
            "--cov=vocutouts",
            "--cov-branch",
            "--cov-report=",
            *session.posargs,
            env={
                "CUTOUT_SERVICE_ACCOUNT": "vo-cutouts@example.com",
                "CUTOUT_SLACK_WEBHOOK": "https://example.com/fake-webhook",
                "CUTOUT_STORAGE_URL": "s3://some-bucket",
                "CUTOUT_WOBBLY_URL": "https://example.com/wobbly",
                **containers.env,
            },
        )


@session(uv_groups=["dev", "typing"])
def typing(session: nox.Session) -> None:
    """Run mypy."""
    session.run(
        "mypy",
        *session.posargs,
        "noxfile.py",
        "src",
        "tests",
    )
