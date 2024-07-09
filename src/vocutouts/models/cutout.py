"""Representation of request parameters.

This is the model used by the user-facing API. These models are converted into
domain models for passing to the backend.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from itertools import batched
from typing import Literal, Self

from astropy import units as u
from astropy.coordinates import Angle, SkyCoord
from pydantic import (
    BaseModel,
    Field,
    SerializeAsAny,
    ValidationError,
    field_validator,
)

from ..exceptions import InvalidCutoutParameterError
from ..uws.config import ParametersModel
from ..uws.exceptions import MultiValuedParameterError
from ..uws.models import UWSJobParameter
from .domain.cutout import (
    WorkerCircleStencil,
    WorkerCutout,
    WorkerPolygonStencil,
    WorkerRangeStencil,
    WorkerStencil,
)

__all__ = [
    "CircleStencil",
    "CutoutParameters",
    "Point",
    "PolygonStencil",
    "Range",
    "RangeStencil",
]


class Point(BaseModel):
    """Represents a point in the sky."""

    ra: float = Field(..., title="ICRS ra in degrees")

    dec: float = Field(..., title="ICRS dec in degrees")


class Range(BaseModel):
    """Represents a range of values."""

    min: float = Field(..., title="Minimum value")

    max: float = Field(..., title="Maximum value")


class Stencil(BaseModel, ABC):
    """Base class for a stencil parameter."""

    type: str = Field(..., title="Type of stencil")

    @classmethod
    @abstractmethod
    def from_string(cls, params: str) -> Self:
        """Create a stencil from its string representation.

        Parameters
        ----------
        params
            String representation of the parameters of the stencil.
        """

    @abstractmethod
    def to_worker_stencil(self) -> WorkerStencil:
        """Convert to the domain model used by the backend worker."""


class CircleStencil(Stencil):
    """Represents a ``CIRCLE`` or ``POS=CIRCLE`` stencil."""

    type: Literal["circle"] = "circle"

    center: Point = Field(
        ..., title="Center", description="Center of the circle"
    )

    radius: float = Field(
        ..., title="Radius", description="Radius of the circle"
    )

    @classmethod
    def from_string(cls, params: str) -> Self:
        ra, dec, radius = (float(p) for p in params.split())
        return cls(center=Point(ra=ra, dec=dec), radius=radius)

    def to_worker_stencil(self) -> WorkerStencil:
        return WorkerCircleStencil(
            center=SkyCoord(
                self.center.ra * u.degree,
                self.center.dec * u.degree,
                frame="icrs",
            ),
            radius=Angle(self.radius * u.degree),
        )


class PolygonStencil(Stencil):
    """Represents a ``POLYGON`` or ``POS=POLYGON`` stencil."""

    type: Literal["polygon"] = "polygon"

    vertices: list[Point] = Field(
        ...,
        title="Vertices of polygon",
        description=(
            "Polygon winding must be counter-clockwise when viewed from the"
            " origin towards the sky."
        ),
    )

    @field_validator("vertices")
    @classmethod
    def _validate_vertices(cls, v: list[Point]) -> list[Point]:
        """Ensure there are at least three vertices."""
        if len(v) < 3:
            raise ValueError("Polygon must have at least three vertices")
        return v

    @classmethod
    def from_string(cls, params: str) -> Self:
        data = [float(p) for p in params.split()]
        if len(data) % 2 != 0:
            msg = f"Odd number of coordinates in vertex list {params}"
            raise ValueError(msg)
        if len(data) < 6:
            msg = "Polygons require at least three vertices"
            raise ValueError(msg)
        return cls(vertices=[Point(ra=r, dec=d) for r, d in batched(data, 2)])

    def to_worker_stencil(self) -> WorkerStencil:
        ras = [v.ra for v in self.vertices]
        decs = [v.dec for v in self.vertices]
        return WorkerPolygonStencil(
            vertices=SkyCoord(ras * u.degree, decs * u.degree, frame="icrs")
        )


class RangeStencil(Stencil):
    """Represents a ``POS=RANGE`` stencil."""

    type: Literal["range"] = "range"

    ra: Range = Field(
        ...,
        title="ra range",
        description="Range of ra values, using inf or -inf for open ranges",
    )

    dec: Range = Field(
        ...,
        title="dec range",
        description="Range of dec values, using inf or -info for open ranges",
    )

    @classmethod
    def from_string(cls, params: str) -> Self:
        ra_min, ra_max, dec_min, dec_max = (float(p) for p in params.split())
        return cls(
            ra=Range(min=ra_min, max=ra_max),
            dec=Range(min=dec_min, max=dec_max),
        )

    def to_worker_stencil(self) -> WorkerStencil:
        return WorkerRangeStencil(
            ra=(self.ra.min, self.ra.max),
            dec=(self.dec.min, self.dec.max),
        )


class CutoutParameters(ParametersModel[WorkerCutout]):
    """Parameters to a cutout request."""

    ids: list[str] = Field(
        ...,
        title="Dataset IDs",
        description=(
            "Dataset IDs on which to operate. Currently, only one dataset ID"
            " is permitted."
        ),
        min_length=1,
        max_length=1,
    )

    stencils: SerializeAsAny[list[Stencil]] = Field(
        ...,
        title="Cutout stencils",
        description=(
            "Cutout stencils describing the desired cutouts. Currently, only"
            " one stencil is supported."
        ),
        min_length=1,
        max_length=1,
    )

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
            return cls(ids=ids, stencils=stencils)
        except ValidationError as e:
            raise InvalidCutoutParameterError(str(e), params) from e

    def to_worker_parameters(self) -> WorkerCutout:
        """Convert to the domain model used by the backend worker."""
        stencils = [s.to_worker_stencil() for s in self.stencils]
        return WorkerCutout(dataset_ids=self.ids, stencils=stencils)

    @staticmethod
    def _parse_stencil(stencil_type: str, params: str) -> Stencil:
        """Convert a string stencil parameter to its representation."""
        if stencil_type == "POS":
            stencil_type, params = params.split(None, 1)

        # Range stencils are not currently supported by the backend and are
        # therefore intentionally left out of this match.
        match stencil_type:
            case "CIRCLE":
                return CircleStencil.from_string(params)
            case "POLYGON":
                return PolygonStencil.from_string(params)
            case _:
                raise ValueError(f"Unknown stencil type {stencil_type}")
