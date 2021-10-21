"""Butler queries for job results.

Job results are stored in a Butler collection, but UWS requires they be
returned to the user as a URL.  This translation layer asks the Butler for the
URL of the underlying file.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lsst.daf.butler import Butler

if TYPE_CHECKING:
    from .models import JobResult

__all__ = ["UWSButler"]


class UWSButler:
    """Wrapper around Butler to ask questions of interest to UWS.

    Parameters
    ----------
    repository : `str`
        The Butler repository to use.
    """

    def __init__(self, repository: str) -> None:
        self._butler = Butler(repository)

    def url_for_result(self, result: JobResult) -> str:
        """Query Butler for the URL for a job result."""
        return self._butler.getURI(
            result.datatype,
            dataId=result.data_id,
            collections=[result.collection],
        ).geturl()
