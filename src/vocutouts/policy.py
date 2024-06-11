"""UWS policy layer for image cutouts."""

from __future__ import annotations

from safir.arq import ArqQueue, JobMetadata
from structlog.stdlib import BoundLogger

from .exceptions import InvalidCutoutParameterError
from .models.parameters import CutoutParameters
from .models.stencils import RangeStencil
from .uws.exceptions import MultiValuedParameterError, ParameterError
from .uws.models import UWSJob, UWSJobParameter
from .uws.policy import UWSPolicy

__all__ = ["ImageCutoutPolicy"]


class ImageCutoutPolicy(UWSPolicy):
    """Policy layer for dispatching and approving changes to UWS jobs.

    For now, rejects all changes to destruction and execution duration by
    returning their current values.

    Parameters
    ----------
    config
         UWS configuration.
    logger
         Logger to use to report errors when dispatching the request.
    """

    def __init__(self, arq: ArqQueue, logger: BoundLogger) -> None:
        super().__init__(arq)
        self._logger = logger

    async def dispatch(self, job: UWSJob, token: str) -> JobMetadata:
        """Dispatch a cutout request to the backend.

        Parameters
        ----------
        job
            The submitted job description.
        token
            Gafaelfawr token used to authenticate to the Butler server in the
            backend.

        Returns
        -------
        JobMetadata
            Metadata about the running job.

        Notes
        -----
        Currently, only one dataset ID and only one stencil are supported.
        This limitation is expected to be relaxed in a later version.
        """
        params = CutoutParameters.from_job_parameters(job.parameters)
        return await self.arq.enqueue(
            "cutout", job.job_id, params.ids, params.stencils, token=token
        )

    def validate_params(self, params: list[UWSJobParameter]) -> None:
        try:
            cutout_params = CutoutParameters.from_job_parameters(params)
        except InvalidCutoutParameterError as e:
            raise ParameterError(str(e)) from e

        # For now, only support a single ID and stencil.
        if len(cutout_params.ids) != 1:
            raise MultiValuedParameterError("Only one ID supported")
        if len(cutout_params.stencils) != 1:
            raise MultiValuedParameterError("Only one stencil is supported")

        # For now, range stencils are not supported.
        stencil = cutout_params.stencils[0]
        if isinstance(stencil, RangeStencil):
            raise ParameterError("RANGE stencils are not supported")
