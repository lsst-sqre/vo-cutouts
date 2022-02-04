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
from typing import TYPE_CHECKING
from uuid import UUID

import astropy.units as u
import dramatiq
from astropy.coordinates import Angle, SkyCoord
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import CurrentMessage
from dramatiq.results import Results
from dramatiq.results.backends import RedisBackend
from lsst.daf.butler import Butler
from lsst.image_cutout_backend import ImageCutoutBackend, projection_finders
from lsst.image_cutout_backend.stencils import (
    SkyCircle,
    SkyPolygon,
    SkyStencil,
)

if TYPE_CHECKING:
    from typing import Any, Dict, List

redis_host = os.environ["CUTOUT_REDIS_HOST"]
redis_password = os.getenv("CUTOUT_REDIS_PASSWORD")
broker = RedisBroker(host=redis_host, password=redis_password)
results = RedisBackend(host=redis_host, password=redis_password)
dramatiq.set_broker(broker)
broker.add_middleware(CurrentMessage())
broker.add_middleware(Results(backend=results))

repository = os.environ["CUTOUT_BUTLER_REPOSITORY"]
output_root = os.environ["CUTOUT_STORAGE_URL"]
tmpdir = os.environ.get("CUTOUT_TMPDIR", "/tmp")
butler = Butler(repository)
projection_finder = projection_finders.ProjectionFinder.make_default()
backend = ImageCutoutBackend(butler, projection_finder, output_root, tmpdir)


# Stubs for other actors implemented elsewhere that workers may call.


@dramatiq.actor(queue_name="uws")
def job_started(job_id: str, message_id: str, start_time: str) -> None:
    pass


@dramatiq.actor(queue_name="uws")
def job_completed(
    message: Dict[str, Any], result: List[Dict[str, str]]
) -> None:
    pass


@dramatiq.actor(queue_name="uws")
def job_failed(message: Dict[str, Any], exception: Dict[str, str]) -> None:
    pass


# Exceptions of these names are handled specially by job_failed.


class TaskFatalError(Exception):
    """Parameters of task are invalid."""


class TaskTransientError(Exception):
    """Some transient problem occurred."""


@dramatiq.actor(queue_name="cutout", max_retries=1)
def cutout(
    job_id: str,
    dataset_ids: List[str],
    stencils: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    """Perform a cutout.

    This is a queue worker for the vo-cutouts service.  It takes a serialized
    cutout request, converts it into a suitable in-memory format, and then
    dispatches it to the scientific code that performs the cutout.  The
    results are stored in a GCS bucket, and the details of the output are
    returned as the result of the worker.

    Parameters
    ----------
    job_id : `str`
        The UWS job ID, used as the key for storing results.
    dataset_ids : List[`str`]
        The data objects on which to perform cutouts.  These are opaque
        identifiers passed as-is to the backend.  The user will normally
        discover them via some service such as ObsTAP.
    stencils : List[Dict[`str`, Any]]
        Serialized stencils for the cutouts to perform.  These are
        JSON-serializable (a requirement for Dramatiq) representations of the
        `~vocutouts.models.stencils.Stencil` objects corresponding to the
        user's request.

    Returns
    -------
    result : List[Dict[`str`, `str`]]
        The results of the job.  This must be a list of dict representations
        of `~vocutouts.uws.models.JobResult` objects.
    """
    # Tell UWS that we have started executing.
    message = CurrentMessage.get_current_message()
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    job_started.send(job_id, message.message_id, now)

    # Currently, only a single data ID and a single stencil are supported.
    assert len(dataset_ids) == 1
    assert len(stencils) == 1

    # Convert the stencils to SkyStencils.
    sky_stencils: List[SkyStencil] = []
    for stencil_dict in stencils:
        if stencil_dict["type"] == "circle":
            center = SkyCoord(
                stencil_dict["center"]["ra"] * u.degree,
                stencil_dict["center"]["dec"] * u.degree,
                frame="icrs",
            )
            radius = Angle(stencil_dict["radius"] * u.degree)
            stencil = SkyCircle.from_astropy(center, radius)
        elif stencil_dict["type"] == "polygon":
            ras = [v[0] for v in stencil_dict["vertices"]]
            decs = [v[1] for v in stencil_dict["vertices"]]
            vertices = SkyCoord(ras * u.degree, decs * u.degree, frame="icrs")
            stencil = SkyPolygon.from_astropy(vertices)
        else:
            msg = f'UsageError Unknown stencil type {stencil_dict["type"]}'
            raise TaskFatalError(msg)
        sky_stencils.append(stencil)

    # Perform the cutout.
    try:
        result = backend.process_uuid(UUID(dataset_ids[0]), sky_stencils[0])
    except Exception as e:
        msg = f"Error Cutout processing failed\n{type(e).__name__}: {str(e)}"
        raise TaskTransientError(msg)

    # Return the result URL.  This must be a dict representation of a
    # vocutouts.uws.models.JobResult.
    result_scheme = result.geturl().scheme
    if result_scheme != "s3":
        msg = f"Error Backend returned URL with scheme {result_scheme}, not s3"
        raise TaskTransientError(msg)
    return [
        {
            "result_id": "cutout",
            "mime_type": "application/fits",
            "url": result.geturl(),
        }
    ]
