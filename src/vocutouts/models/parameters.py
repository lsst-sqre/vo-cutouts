"""Representation of request parameters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from ..exceptions import InvalidCutoutParameterError
from ..uws.models import UWSJobParameter
from .stencils import Stencil, parse_stencil


@dataclass
class CutoutParameters:
    """The parameters to a cutout request."""

    ids: list[str]
    """The dataset IDs on which to operate."""

    stencils: list[Stencil]
    """The cutout stencils to apply."""

    @classmethod
    def from_job_parameters(cls, params: list[UWSJobParameter]) -> Self:
        """Convert generic UWS parameters to the iamge cutout parameters.

        Parameters
        ----------
        params
            Generic input job parameters.

        Returns
        -------
        CutoutParameters
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
            msg = f"Invalid cutout parameter: {type(e).__name__}: {e!s}"
            raise InvalidCutoutParameterError(msg, params) from e
        if not ids:
            raise InvalidCutoutParameterError("No dataset ID given", params)
        if not stencils:
            raise InvalidCutoutParameterError(
                "No cutout stencil given", params
            )
        return cls(ids=ids, stencils=stencils)
