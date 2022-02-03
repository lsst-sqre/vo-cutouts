"""Retrieval of job results.

Job results are stored in a Google Cloud Storage bucket, but UWS requires they
be returned to the user as a URL.  This translation layer converts the ``s3``
URL to a signed URL suitable for returning to a client of the service.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from google.cloud import storage

from .models import JobResultURL

if TYPE_CHECKING:
    from .config import UWSConfig
    from .models import JobResult

__all__ = ["ResultStore"]


class ResultStore:
    """Result storage handling.

    Parameters
    ----------
    config : `vocutouts.uws.config.UWSConfig`
        The UWS configuration.
    """

    def __init__(self, config: UWSConfig) -> None:
        self._config = config
        self._gcs = storage.Client()

    async def url_for_result(self, result: JobResult) -> JobResultURL:
        """Convert a job result into a signed URL."""
        uri = urlparse(result.url)
        assert uri.scheme == "s3"
        bucket = self._gcs.bucket(uri.netloc)
        blob = bucket.blob(uri.path[1:])
        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=self._config.url_lifetime),
            method="GET",
            response_type=result.mime_type,
        )

        # Return the JobResultURL representation of this result.
        return JobResultURL(
            result_id=result.result_id,
            url=signed_url,
            size=result.size,
            mime_type=result.mime_type,
        )
