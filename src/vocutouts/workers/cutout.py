"""Worker for SODA cutouts.

This is a standalone file intended to be injected into a stack container as
the arq worker definition. Only this module is allowed to use stack packages.
"""

from __future__ import annotations

import os
from datetime import timedelta
from urllib.parse import urlparse
from uuid import UUID

import structlog
from lsst.afw.geom import SinglePolygonException
from lsst.daf.butler import LabeledButlerFactory
from lsst.image_cutout_backend import ImageCutoutBackend, projection_finders
from lsst.image_cutout_backend.stencils import SkyCircle, SkyPolygon
from safir.arq import ArqMode
from safir.logging import configure_logging
from structlog.stdlib import BoundLogger

from ..models.domain.cutout import (
    WorkerCircleStencil,
    WorkerCutout,
    WorkerPolygonStencil,
)
from ..uws.uwsworker import (
    WorkerConfig,
    WorkerFatalError,
    WorkerJobInfo,
    WorkerResult,
    WorkerUsageError,
    build_worker,
)

_BUTLER_FACTORY = LabeledButlerFactory()
"""Factory for creating Butler objects."""

__all__ = ["WorkerSettings"]


def _get_backend(butler_label: str, token: str) -> ImageCutoutBackend:
    """Given the Butler label, retrieve or build a backend.

    The dataset ID will be a URI of the form ``butler://<label>/<uuid>``.
    Each label corresponds to a different Butler and thus a different backend,
    which are cached in `BACKENDS`.

    Parameters
    ----------
    butler_label
        Label portion of the Butler URI.
    token
        Gafaelfawr token, used to access the Butler service.

    Returns
    -------
    lsst.image_cutout_backend.ImageCutoutBackend
        Backend to use.
    """
    butler = _BUTLER_FACTORY.create_butler(
        label=butler_label, access_token=token
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
    params: WorkerCutout, info: WorkerJobInfo, logger: BoundLogger
) -> list[WorkerResult]:
    """Perform a cutout.

    This is a queue worker for the vo-cutouts service. It takes a serialized
    cutout request, converts it into a suitable in-memory format, and then
    dispatches it to the scientific code that performs the cutout. The results
    are stored in a GCS bucket, and the details of the output are returned as
    the result of the worker.

    Parameters
    ----------
    params
        Cutout parameters.
    info
        Information about the UWS job we're executing.
    logger
        Logger to use for logging.

    Returns
    -------
    list of WorkerResult
        Results of the job.

    Raises
    ------
    WorkerFatalError
        Raised if the cutout failed for unknown reasons, or due to internal
        errors. This is the normal failure exception, since we usually do not
        know why the backend code failed and make the pessimistic assumption
        that the failure is not transient.
    WorkerUsageError
        Raised if the cutout failed due to deficiencies in the parameters
        submitted by the user that could not be detected by the frontend
        service.
    """
    # Currently, only a single dataset ID and a single stencil are supported.
    # These constraints should have been enforced by the web service, so if we
    # see them here, there's some sort of internal bug.
    if len(params.dataset_ids) != 1:
        msg = "Internal error: only one dataset ID supported"
        raise WorkerFatalError(msg)
    if len(params.stencils) != 1:
        msg = "Internal error: only one stencil supported"
        raise WorkerFatalError(msg)

    # Parse the dataset ID and retrieve an appropriate backend.
    butler_label, uuid = _parse_uri(params.dataset_ids[0])
    backend = _get_backend(butler_label, info.token)

    # Convert the stencils to SkyStencils.
    sky_stencils = []
    for stencil in params.stencils:
        match stencil:
            case WorkerCircleStencil(center=center, radius=radius):
                sky_stencil = SkyCircle.from_astropy(center, radius, clip=True)
            case WorkerPolygonStencil(vertices=vertices):
                sky_stencil = SkyPolygon.from_astropy(vertices, clip=True)
            case _:
                type_str = type(stencil).__name__
                msg = f"Internal error: unknown stencil type {type_str}"
                logger.warning(msg)
                raise WorkerFatalError(msg)
        sky_stencils.append(sky_stencil)

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
        raise WorkerUsageError(
            "No intersection between cutout and image", add_traceback=True
        ) from e
    except Exception as e:
        raise WorkerFatalError(
            "Cutout processing failed", str(e), add_traceback=True
        ) from e

    # Return the result.
    result_url = result.geturl()
    result_scheme = urlparse(result_url).scheme
    if result_scheme not in ("gs", "s3"):
        msg = f"Backend returned URL with scheme {result_scheme}, not gs or s3"
        raise WorkerFatalError(msg, f"URL: {result_url}")
    logger.info("Cutout successful")
    return [
        WorkerResult(
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
    WorkerConfig(
        arq_mode=ArqMode.production,
        arq_queue_url=os.environ["CUTOUT_ARQ_QUEUE_URL"],
        arq_queue_password=os.getenv("CUTOUT_ARQ_QUEUE_PASSWORD"),
        timeout=timedelta(seconds=int(os.environ["CUTOUT_TIMEOUT"])),
    ),
    structlog.get_logger("vocutouts"),
)
"""arq configuration for the cutout worker."""
