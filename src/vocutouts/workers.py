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

import math
import os
import subprocess
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import astropy.units as u
import dramatiq
from astropy.coordinates import SkyCoord
from astropy.table import QTable
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import CurrentMessage
from dramatiq.results import Results
from dramatiq.results.backends import RedisBackend
from lsst.daf.butler import Butler, DatasetType

if TYPE_CHECKING:
    from typing import Any, Dict, List

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


@dramatiq.actor(queue_name="cutout")
def cutout_range(
    job_id: str,
    data_id: Dict[str, str],
    ra_min: float,
    ra_max: float,
    dec_min: float,
    dec_max: float,
) -> List[Dict[str, Any]]:
    """Implement a cutout for a specified range."""
    # Tell UWS that we have started executing.
    message = CurrentMessage.get_current_message()
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    job_started.send(job_id, message.message_id, now)

    # Input parameters to the pipeline will be provided as a Butler repository
    # containing a dataset of type cutout_positions and a data ID matching the
    # image to which the cutout stencil is being applied.  Create that input
    # Butler collection and register the data type.  Also determine the name
    # of the output collection.
    uuid = uuid4()
    input_collection = f"service/cutouts/positions/{uuid}"
    output_collection = f"service/cutouts/{uuid}"
    repository = os.environ["CUTOUT_BUTLER_REPOSITORY"]
    collection = os.environ["CUTOUT_BUTLER_COLLECTION"]
    butler = Butler(repository, writeable=True, run=input_collection)
    dataset_type = DatasetType(
        "cutout_positions",
        dimensions=data_id.keys(),
        universe=butler.registry.dimensions,
        storageClass="AstropyQTable",
    )
    butler.registry.registerDatasetType(dataset_type)

    # Store the cutout parameters in the input Butler collection.
    #
    # Use a conversion ratio of 0.2 arcsec/pixel so that we can use the
    # current test cutout pipeline.  The eventual cutout pipeline will accept
    # min/max pairs in ICRS coordinates.
    if any(math.isinf(v) for v in (ra_min, ra_max, dec_min, dec_max)):
        raise TaskFatalError("UsageError Unbounded ranges not yet supported")
    pos = SkyCoord(ra_min, dec_min, unit="deg")
    xspan = ((ra_max - ra_min) * 3600 / 0.2) * u.dimensionless_unscaled
    yspan = ((dec_max - dec_min) * 3600 / 0.2) * u.dimensionless_unscaled
    row = 1 * u.dimensionless_unscaled
    input_table = QTable(
        [[row], [pos], [xspan], [yspan]],
        names=["id", "position", "xspan", "yspan"],
    )
    butler.put(input_table, "cutout_positions", **data_id)

    # Perform the cutout.
    data_query = " AND ".join(f"{k}='{v}'" for k, v in data_id.items())
    result = subprocess.run(
        [
            "pipetask",
            "run",
            "-j",
            "1",
            "-b",
            repository,
            "--register-dataset-types",
            "-t",
            "lsst.pipe.tasks.calexpCutout.CalexpCutoutTask",
            "-d",
            data_query,
            "--output",
            output_collection,
            "-i",
            f"{input_collection},{collection}",
        ],
        capture_output=True,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        msg = (
            f"Error Cutout failed with status {result.returncode}\n"
            f"{result.stdout.decode()}"
        )
        raise TaskTransientError(msg)

    # Return a pointer to the result Butler collection.  This must be a dict
    # representation of a vocutouts.uws.models.JobResult.
    return [
        {
            "result_id": "cutout",
            "collection": output_collection,
            "data_id": data_id,
            "datatype": "calexp_cutouts",
            "mime_type": "application/fits",
        }
    ]
