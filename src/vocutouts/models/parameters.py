"""Representation of request parameters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..exceptions import InvalidCutoutParameterError
from .stencils import parse_stencil

if TYPE_CHECKING:
    from typing import List

    from .models.stencils import Stencil
    from .uws.models import JobParameter


@dataclass
class CutoutParameters:
    """The parameters to a cutout request."""

    ids: List[str]
    """The data IDs on which to operate."""

    stencils: List[Stencil]
    """The cutout stencils to apply."""

    @classmethod
    def from_job_parameters(
        cls, params: List[JobParameter]
    ) -> CutoutParameters:
        """Convert generic UWS parameters to the iamge cutout parameters.

        Parameters
        ----------
        params : List[`vocutouts.uws.models.JobParameter`]
            Generic input job parameters.

        Returns
        -------
        reuslt : `CutoutParameters`
            The parsed cutout parameters specific to the image cutout service.

        Raises
        ------
        vocutouts.exceptions.InvalidCutoutParameterError
            One of the parameters could not be parsed.
        """
        ids = []
        stencils = []
        try:
            for param in params:
                if param.parameter_id == "id":
                    ids.append(param.value)
                else:
                    f = parse_stencil(param.parameter_id.upper(), param.value)
                    stencils.append(f)
        except Exception as e:
            msg = f"Invalid cutout parameter: {type(e).__name__}: {str(e)}"
            raise InvalidCutoutParameterError(msg, params) from e
        if not ids:
            raise InvalidCutoutParameterError("No data ID given", params)
        if not stencils:
            raise InvalidCutoutParameterError(
                "No cutout stencil given", params
            )
        return cls(ids=ids, stencils=stencils)
