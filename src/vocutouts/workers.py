"""Cutout backend worker definitions.

This is a standalone file intended to be injected into a stack container as
the Dramatiq worker definitions.  It must define its own broker and stub
actors for the UWS database update actors that it may call.  Only this module
is allowed to use stack packages.

Notes
-----
The call signatures of the actors defined here must exactly match the call
signatures defined in :py:mod:`vocutouts.actors`, which will be used by the
API frontend to dispatch jobs.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import astropy.units as u
import dramatiq
import structlog
from astropy.coordinates import Angle, SkyCoord
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import CurrentMessage
from dramatiq.results import Results
from dramatiq.results.backends import RedisBackend
from lsst.daf.butler import LabeledButlerFactory
from lsst.image_cutout_backend import ImageCutoutBackend, projection_finders
from lsst.image_cutout_backend.stencils import (
    SkyCircle,
    SkyPolygon,
    SkyStencil,
)
from safir.logging import configure_logging

BUTLER_FACTORY = LabeledButlerFactory()

configure_logging(
    name=os.getenv("SAFIR_LOGGER", "vocutouts"),
    profile=os.getenv("SAFIR_PROFILE", "production"),
    log_level=os.getenv("SAFIR_LOG_LEVEL", "INFO"),
    add_timestamp=True,
)
redis_host = os.environ["CUTOUT_REDIS_HOST"]
redis_password = os.getenv("CUTOUT_REDIS_PASSWORD")
broker = RedisBroker(host=redis_host, password=redis_password)
results = RedisBackend(host=redis_host, password=redis_password)
dramatiq.set_broker(broker)
broker.add_middleware(CurrentMessage())
broker.add_middleware(Results(backend=results))


# Stubs for other actors implemented elsewhere that workers may call.


@dramatiq.actor(queue_name="uws")
def job_started(job_id: str, message_id: str, start_time: str) -> None:
    pass


@dramatiq.actor(queue_name="uws")
def job_completed(
    message: dict[str, Any], result: list[dict[str, str]]
) -> None:
    pass


@dramatiq.actor(queue_name="uws")
def job_failed(message: dict[str, Any], exception: dict[str, str]) -> None:
    pass


# Exceptions of these names are handled specially by job_failed.


class TaskFatalError(Exception):
    """Parameters of task are invalid."""


class TaskTransientError(Exception):
    """Some transient problem occurred."""


def get_backend(butler_label: str, access_token: str) -> ImageCutoutBackend:
    """Given the Butler label, retrieve or build a backend.

    The dataset ID will be a URI of the form ``butler://<label>/<uuid>``.
    Each label corresponds to a different Butler and thus a different backend,
    which are cached in `BACKENDS`.

    Parameters
    ----------
    butler_label
        The label portion fo the Butler URI.

    Returns
    -------
    lsst.image_cutout_backend.ImageCutoutBackend
        Backend to use.
    """

    butler = BUTLER_FACTORY.create_butler(
        label=butler_label, access_token=access_token
    )
    # At present, projection finders and image cutout backend have no internal
    # caching and are cheap to construct, so we just make a new one for each
    # request.
    projection_finder = projection_finders.ProjectionFinder.make_default()
    output = os.environ["CUTOUT_STORAGE_URL"]
    tmpdir = os.environ.get("CUTOUT_TMPDIR", "/tmp")
    return ImageCutoutBackend(butler, projection_finder, output, tmpdir)


def parse_uri(uri: str) -> tuple[str, UUID]:
    """Parse a Butler URI.

    Parameters
    ----------
    uri
        URI to a Butler object.

    Returns
    -------
    str
        The Butler label.
    uuid.UUID
        The object UUID.
    """
    parsed_uri = urlparse(uri)
    return parsed_uri.netloc, UUID(parsed_uri.path[1:])


@dramatiq.actor(queue_name="cutout", max_retries=1, store_results=True)
def cutout(
    job_id: str,
    dataset_ids: list[str],
    stencils: list[dict[str, Any]],
    access_token: str,
) -> list[dict[str, str]]:
    """Perform a cutout.

    This is a queue worker for the vo-cutouts service.  It takes a serialized
    cutout request, converts it into a suitable in-memory format, and then
    dispatches it to the scientific code that performs the cutout.  The
    results are stored in a GCS bucket, and the details of the output are
    returned as the result of the worker.

    Parameters
    ----------
    job_id
        The UWS job ID, used as the key for storing results.
    dataset_ids
        The data objects on which to perform cutouts.  These are opaque
        identifiers passed as-is to the backend.  The user will normally
        discover them via some service such as ObsTAP.
    stencils
        Serialized stencils for the cutouts to perform.  These are
        JSON-serializable (a requirement for Dramatiq) representations of the
        `~vocutouts.models.stencils.Stencil` objects corresponding to the
        user's request.
    access_token
        Gafaelfawr access token used to authenticate to Butler server.

    Returns
    -------
    list of dict of str to str
        The results of the job.  This must be a list of dict representations
        of `~vocutouts.uws.models.JobResult` objects.
    """
    logger = structlog.get_logger(os.getenv("SAFIR_LOGGER", "vocutouts"))
    logger = logger.bind(
        job_id=job_id, dataset_ids=dataset_ids, stencils=stencils
    )

    # Tell UWS that we have started executing.
    message = CurrentMessage.get_current_message()
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    job_started.send(job_id, message.message_id, now)

    # Currently, only a single dataset ID and a single stencil are supported.
    if len(dataset_ids) != 1:
        msg = "Only one dataset ID supported"
        logger.warning(msg)
        raise TaskFatalError(f"UsageError {msg}")
    if len(stencils) != 1:
        msg = "Only one stencil supported"
        logger.warning(msg)
        raise TaskFatalError(f"UsageError {msg}")

    # Parse the dataset ID and retrieve an appropriate backend.
    butler_label, uuid = parse_uri(dataset_ids[0])
    backend = get_backend(butler_label, access_token)

    # Convert the stencils to SkyStencils.
    sky_stencils: list[SkyStencil] = []
    for stencil_dict in stencils:
        if stencil_dict["type"] == "circle":
            center = SkyCoord(
                stencil_dict["center"]["ra"] * u.degree,
                stencil_dict["center"]["dec"] * u.degree,
                frame="icrs",
            )
            radius = Angle(stencil_dict["radius"] * u.degree)
            stencil = SkyCircle.from_astropy(center, radius, clip=True)
        elif stencil_dict["type"] == "polygon":
            ras = [v[0] for v in stencil_dict["vertices"]]
            decs = [v[1] for v in stencil_dict["vertices"]]
            vertices = SkyCoord(ras * u.degree, decs * u.degree, frame="icrs")
            stencil = SkyPolygon.from_astropy(vertices, clip=True)
        else:
            msg = f'Unknown stencil type {stencil_dict["type"]}'
            logger.warning(msg)
            raise TaskFatalError(f"UsageError {msg}")
        sky_stencils.append(stencil)

    # Perform the cutout.
    logger.info("Starting cutout request")
    try:
        result = backend.process_uuid(sky_stencils[0], uuid, mask_plane=None)
    except Exception as e:
        logger.exception("Cutout processing failed")
        msg = f"Error Cutout processing failed\n{type(e).__name__}: {str(e)}"
        raise TaskTransientError(msg)

    # Return the result URL.  This must be a dict representation of a
    # vocutouts.uws.models.JobResult.
    result_url = result.geturl()
    result_scheme = urlparse(result_url).scheme
    if result_scheme != "s3":
        msg = f"Backend returned URL with scheme {result_scheme}, not s3"
        logger.error(msg)
        raise TaskTransientError(f"Error {msg}")
    logger.info("Cutout successful")
    return [
        {
            "result_id": "cutout",
            "mime_type": "application/fits",
            "url": result_url,
        }
    ]
