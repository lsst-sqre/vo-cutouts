"""Butler queries for job results.

Job results are stored in a Butler collection, but UWS requires they be
returned to the user as a URL.  This translation layer asks the Butler for the
URL of the underlying file.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from google.cloud import storage
from lsst.daf.butler import Butler

from .models import JobResultURL

if TYPE_CHECKING:
    from .config import UWSConfig
    from .models import JobResult

__all__ = ["UWSButler"]


class UWSButler:
    """Wrapper around Butler to ask questions of interest to UWS.

    Parameters
    ----------
    repository : `str`
        The Butler repository to use.
    """

    def __init__(self, repository: str, config: UWSConfig) -> None:
        self._butler = Butler(repository)
        self._config = config
        self._gcs = storage.Client()

    async def url_for_result(self, result: JobResult) -> JobResultURL:
        """Query Butler for the URL for a job result.

        Notes
        -----
        Currently, this is synchronous, blocking the rest of the cutout
        frontend, and probably painfully slow (although I've not gotten hard
        measurements yet).  Butler has to be refreshed to pick up new
        collections (and, the first time we do a cutout, the new data type),
        which does synchronous database calls.  Butler is not designed to be
        used concurrently (so far as I know) so running these calls in a
        thread pool is probably unsafe.

        This is a stop-gap until we have client/server Butler, so perhaps we
        can live with it until then.
        """
        self._butler.registry.refresh()
        uri = self._butler.getURI(
            result.datatype,
            dataId=result.data_id,
            collections=[result.collection],
        )

        # Generate a signed URL for the result.  Eventually, we may need to
        # cache these so that we don't regenerate them on every retrieval of
        # the results.
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
