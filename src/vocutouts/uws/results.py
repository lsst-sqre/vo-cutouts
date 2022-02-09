"""Retrieval of job results.

Job results are stored in a Google Cloud Storage bucket, but UWS requires they
be returned to the user as a URL.  This translation layer converts the ``s3``
URL to a signed URL suitable for returning to a client of the service.
"""

from __future__ import annotations

from datetime import timedelta
from urllib.parse import urlparse

import google.auth
from google.auth import impersonated_credentials
from google.cloud import storage

from .config import UWSConfig
from .models import JobResult, JobResultURL

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
        self._credentials, _ = google.auth.default()
        self._gcs = storage.Client()

    async def url_for_result(self, result: JobResult) -> JobResultURL:
        """Convert a job result into a signed URL.

        Notes
        -----
        This uses custom credentials so that it will work with a GKE service
        account without having to export the secret key as a JSON blob and
        manage it as a secret.  For more information, see
        `gcs_signedurl <https://github.com/salrashid123/gcs_signedurl>`__.

        This is probably too inefficient, since it gets new signing
        credentials each time it generates a signed URL.  Doing better will
        require figuring out the lifetime and refreshing the credentials when
        the lifetime has expired, which in turn will probably require a
        longer-lived object to hold the credentials.
        """
        uri = urlparse(result.url)
        assert uri.scheme == "s3"
        bucket = self._gcs.bucket(uri.netloc)
        blob = bucket.blob(uri.path[1:])
        signing_credentials = impersonated_credentials.Credentials(
            source_credentials=self._credentials,
            target_principal=self._config.signing_service_account,
            target_scopes=(
                "https://www.googleapis.com/auth/devstorage.read_only"
            ),
            lifetime=2,
        )
        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=self._config.url_lifetime),
            method="GET",
            response_type=result.mime_type,
            credentials=signing_credentials,
        )

        # Return the JobResultURL representation of this result.
        return JobResultURL(
            result_id=result.result_id,
            url=signed_url,
            size=result.size,
            mime_type=result.mime_type,
        )
