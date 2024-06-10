"""Worker for SODA cutouts.

This is a standalone file intended to be injected into a stack container as
the arq worker definition. Only this module is allowed to use stack packages.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import astropy.units as u
import structlog
from astropy.coordinates import Angle, SkyCoord
from lsst.afw.geom import SinglePolygonException
from lsst.daf.butler import LabeledButlerFactory
from lsst.image_cutout_backend import ImageCutoutBackend, projection_finders
from lsst.image_cutout_backend.stencils import SkyCircle, SkyPolygon
from safir.arq import ArqMode
from safir.logging import configure_logging
from structlog.stdlib import BoundLogger

from ..uws.exceptions import TaskFatalError, TaskUserError
from ..uws.models import ErrorCode, UWSJobResult
from ..uws.workers import UWSWorkerConfig, build_worker

_BUTLER_FACTORY = LabeledButlerFactory()
"""Factory for creating Butler objects."""

__all__ = ["WorkerSettings"]


def _get_backend(butler_label: str, access_token: str) -> ImageCutoutBackend:
    """Given the Butler label, retrieve or build a backend.

    The dataset ID will be a URI of the form ``butler://<label>/<uuid>``.
    Each label corresponds to a different Butler and thus a different backend,
    which are cached in `BACKENDS`.

    Parameters
    ----------
    butler_label
        Label portion of the Butler URI.
    access_token
        Gafaelfawr access token, used to access the Butler service.

    Returns
    -------
    lsst.image_cutout_backend.ImageCutoutBackend
        Backend to use.
    """
    butler = _BUTLER_FACTORY.create_butler(
        label=butler_label, access_token=access_token
    )

    # At present, projection finders and image cutout backend have no internal
    # caching and are cheap to construct, so we just make a new one for each
    # request.
    projection_finder = projection_finders.ProjectionFinder.make_default()
    output = os.environ["CUTOUT_STORAGE_URL"]
    tmpdir = os.environ.get("CUTOUT_TMPDIR", "/tmp")
    return ImageCutoutBackend(butler, projection_finder, output, tmpdir)


def _parse_uri(uri: str) -> tuple[str, UUID]:
    """Parse a Butler URI.

    Parameters
    ----------
    uri
        URI to a Butler object.

    Returns
    -------
    str
        Butler label.
    UUID
        Object UUID.
    """
    parsed_uri = urlparse(uri)
    return parsed_uri.netloc, UUID(parsed_uri.path[1:])


def cutout(
    job_id: str,
    dataset_ids: list[str],
    stencils: list[dict[str, Any]],
    access_token: str,
    *,
    logger: BoundLogger,
) -> list[UWSJobResult]:
    """Perform a cutout.

    This is a queue worker for the vo-cutouts service. It takes a serialized
    cutout request, converts it into a suitable in-memory format, and then
    dispatches it to the scientific code that performs the cutout. The results
    are stored in a GCS bucket, and the details of the output are returned as
    the result of the worker.

    Parameters
    ----------
    job_id
        UWS job ID, used as the key for storing results.
    dataset_ids
        Data objects on which to perform cutouts. These are opaque identifiers
        passed as-is to the backend. The user will normally discover them via
        some service such as ObsTAP.
    stencils
        Serialized stencils for the cutouts to perform. These are
        `~vocutouts.models.stencils.Stencil` objects corresponding to the
        user's request.
    access_token
        Gafaelfawr access token used to authenticate to Butler server.
    logger
        Logger to use for logging.

    Returns
    -------
    list of UWSJobResult
        Results of the job.

    Raises
    ------
    TaskFatalError
        Raised if the cutout failed for reasons that are unlikely to be fixed
        on retry, such as syntax errors.
    TaskTransientError
        Raised if the cutout failed for reasons that may go away if the cutout
        is retried.
    """
    logger = logger.bind(dataset_ids=dataset_ids, stencils=stencils)

    # Currently, only a single dataset ID and a single stencil are supported.
    # These constraints should have been applied by the policy layer, so if we
    # see them here, there's some bug in the cutout service.
    if len(dataset_ids) != 1:
        msg = "Only one dataset ID supported"
        raise TaskFatalError(ErrorCode.USAGE_ERROR, msg)
    if len(stencils) != 1:
        msg = "Only one stencil supported"
        raise TaskFatalError(ErrorCode.USAGE_ERROR, msg)

    # Parse the dataset ID and retrieve an appropriate backend.
    butler_label, uuid = _parse_uri(dataset_ids[0])
    backend = _get_backend(butler_label, access_token)

    # Convert the stencils to SkyStencils.
    sky_stencils = []
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
            raise TaskFatalError(ErrorCode.USAGE_ERROR, msg)
        sky_stencils.append(stencil)

    # Perform the cutout. We have no idea if unknown exceptions here are
    # transient or fatal, so conservatively assume they are fatal. Provide a
    # traceback in the error details to give the user more of a chance at
    # understanding the problem, and hope it doesn't contain any
    # security-sensitive data. (When running with workload identity, it really
    # shoudln't.)
    logger.info("Starting cutout request")
    try:
        result = backend.process_uuid(sky_stencils[0], uuid, mask_plane=None)
    except SinglePolygonException as e:
        raise TaskUserError(
            ErrorCode.USAGE_ERROR,
            "No intersection between cutout and image",
            str(e),
            add_traceback=True,
        ) from e
    except Exception as e:
        raise TaskFatalError(
            ErrorCode.ERROR, "Cutout processing failed", add_traceback=True
        ) from e

    # Return the result.
    result_url = result.geturl()
    result_scheme = urlparse(result_url).scheme
    if result_scheme not in ("gs", "s3"):
        msg = f"Backend returned URL with scheme {result_scheme}, not gs or s3"
        raise TaskFatalError(ErrorCode.ERROR, msg, f"URL: {result_url}")
    logger.info("Cutout successful")
    return [
        UWSJobResult(
            result_id="cutout", url=result_url, mime_type="application/fits"
        )
    ]


configure_logging(
    name="vocutouts",
    profile=os.getenv("CUTOUT_PROFILE", "development"),
    log_level=os.getenv("CUTOUT_LOG_LEVEL", "INFO"),
)

WorkerSettings = build_worker(
    cutout,
    UWSWorkerConfig(
        arq_mode=ArqMode.production,
        arq_queue_url=os.environ["CUTOUT_ARQ_QUEUE_URL"],
        arq_queue_password=os.getenv("CUTOUT_ARQ_QUEUE_PASSWORD"),
        timeout=timedelta(seconds=int(os.environ["CUTOUT_TIMEOUT"])),
    ),
    structlog.get_logger("vocutouts"),
)
"""arq configuration for the cutout worker."""
