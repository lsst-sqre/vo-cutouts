"""Retrieval of job results.

Job results are stored in a Google Cloud Storage bucket, but UWS requires they
be returned to the user as a URL.  This translation layer converts the ``s3``
URL to a signed URL suitable for returning to a client of the service.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from google.auth import compute_engine
from google.auth.transport import requests
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
        self._auth_request = requests.Request()
        self._gcs = storage.Client()

    async def url_for_result(self, result: JobResult) -> JobResultURL:
        """Convert a job result into a signed URL.

        Notes
        -----
        This uses custom credentials so that it will work with a GKE service
        account without having to export the secret key as a JSON blob and
        manage it as a secret.  For more information, see
        https://gist.github.com/jezhumble/91051485db4462add82045ef9ac2a0ec
        """
        uri = urlparse(result.url)
        assert uri.scheme == "s3"
        bucket = self._gcs.bucket(uri.netloc)
        blob = bucket.blob(uri.path[1:])
        credentials = compute_engine.IDTokenCredentials(self._auth_request, "")
        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=self._config.url_lifetime),
            method="GET",
            response_type=result.mime_type,
            credentials=credentials,
        )

        # Return the JobResultURL representation of this result.
        return JobResultURL(
            result_id=result.result_id,
            url=signed_url,
            size=result.size,
            mime_type=result.mime_type,
        )
