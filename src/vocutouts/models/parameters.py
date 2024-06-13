"""Representation of request parameters."""

from __future__ import annotations

from typing import Self

from pydantic import ValidationError

from ..exceptions import InvalidCutoutParameterError
from ..uws.config import ParametersModel
from ..uws.exceptions import MultiValuedParameterError
from ..uws.models import UWSJobParameter
from .request import (
    CircleStencil,
    CutoutRequest,
    PolygonStencil,
    RangeStencil,
    Stencil,
)


class CutoutParameters(CutoutRequest, ParametersModel):
    """Parameters to a cutout request."""

    @classmethod
    def from_job_parameters(cls, params: list[UWSJobParameter]) -> Self:
        """Convert generic UWS parameters to the image cutout parameters.

        Parameters
        ----------
        params
            Generic input job parameters.

        Returns
        -------
        CutoutParameters
            Parsed cutout parameters specific to the image cutout service.

        Raises
        ------
        InvalidCutoutParameterError
            Raised if one of the parameters could not be parsed.
        MultiValuedParameterError
            Raised if more than one dataset ID or more than one stencil is
            provided.
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

        # For now, only support a single ID and stencil. These have to be
        # checked outside of the validator because the SODA standard requires
        # returning a different error in this case.
        if len(ids) > 1:
            raise MultiValuedParameterError("Only one ID supported")
        if len(stencils) > 1:
            raise MultiValuedParameterError("Only one stencil is supported")

        try:
            return cls(dataset_ids=ids, stencils=stencils)
        except ValidationError as e:
            raise InvalidCutoutParameterError(str(e), params) from e

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
