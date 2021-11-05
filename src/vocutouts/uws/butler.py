"""Butler queries for job results.

Job results are stored in a Butler collection, but UWS requires they be
returned to the user as a URL.  This translation layer asks the Butler for the
URL of the underlying file.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from functools import partial
from typing import TYPE_CHECKING

from google.cloud import storage
from lsst.daf.butler import Butler, DatasetType

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
        """Query Butler for the URL for a job result."""
        # The dataset type shouldn't be registered every time, but that means
        # we need the dimensions in config.  That will need to be added later.
        dataset_type = DatasetType(
            "calexp_cutouts",
            dimensions=result.data_id.keys(),
            universe=self._butler.registry.dimensions,
            storageClass="Stamps",
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            partial(self._butler.registry.registerDatasetType, dataset_type),
        )
        uri = await loop.run_in_executor(
            None,
            partial(
                self._butler.getURI,
                result.datatype,
                dataId=result.data_id,
                collections=[result.collection],
            ),
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
