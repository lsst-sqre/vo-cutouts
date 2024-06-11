"""Representation of request parameters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from ..exceptions import InvalidCutoutParameterError
from ..uws.models import UWSJobParameter
from .stencils import CircleStencil, PolygonStencil, RangeStencil, Stencil


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
                    stencil_type = param.parameter_id.upper()
                    stencil = cls._parse_stencil(stencil_type, param.value)
                    stencils.append(stencil)
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

    @staticmethod
    def _parse_stencil(stencil_type: str, params: str) -> Stencil:
        """Convert a string stencil parameter to its representation."""
        if stencil_type == "POS":
            stencil_type, params = params.split(None, 1)
        match stencil_type:
            case "CIRCLE":
                return CircleStencil.from_string(params)
            case "POLYGON":
                return PolygonStencil.from_string(params)
            case "RANGE":
                return RangeStencil.from_string(params)
            case _:
                raise ValueError(f"Unknown stencil type {stencil_type}")
