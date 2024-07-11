"""Retrieval of job results.

Job results are stored in a Google Cloud Storage bucket, but UWS requires they
be returned to the user as a URL. This translation layer converts the ``s3``
URL to a signed URL suitable for returning to a client of the service.
"""

from __future__ import annotations

from safir.gcs import SignedURLService

from .config import UWSConfig
from .models import UWSJobResult, UWSJobResultSigned

__all__ = ["ResultStore"]


class ResultStore:
    """Result storage handling.

    Parameters
    ----------
    config
        The UWS configuration.
    """

    def __init__(self, config: UWSConfig) -> None:
        self._config = config
        self._url_service = SignedURLService(
            service_account=config.signing_service_account,
            lifetime=config.url_lifetime,
        )

    def sign_url(self, result: UWSJobResult) -> UWSJobResultSigned:
        """Convert a job result into a signed URL.

        Notes
        -----
        This uses custom credentials so that it will work with a GKE service
        account without having to export the secret key as a JSON blob and
        manage it as a secret. For more information, see `gcs_signedurl
        <https://github.com/salrashid123/gcs_signedurl>`__.

        This is probably too inefficient, since it gets new signing
        credentials each time it generates a signed URL. Doing better will
        require figuring out the lifetime and refreshing the credentials when
        the lifetime has expired, which in turn will probably require a
        longer-lived object to hold the credentials.
        """
        signed_url = self._url_service.signed_url(result.url, result.mime_type)
        return UWSJobResultSigned(
            result_id=result.result_id,
            url=signed_url,
            size=result.size,
            mime_type=result.mime_type,
        )
