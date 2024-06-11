"""Parsing and representation a cutout request.

Note
----
This module must not depend on any other portions of the vo-cutouts
application or on the UWS library apart from the library for UWS backend
workers in order to force too many dependencies on the backend worker. It
should try to use only dependencies present in the stack container.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Annotated, Literal, Self, TypeAlias

from astropy import units as u
from astropy.coordinates import Angle, SkyCoord
from astropy.utils import isiterable
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    SerializeAsAny,
    model_validator,
)

__all__ = [
    "AngleSerializable",
    "CircleStencil",
    "PolygonStencil",
    "Range",
    "RangeStencil",
    "SkyCoordSerializable",
    "Stencil",
]


def _serialize_sky_coord(
    c: SkyCoord,
) -> list[tuple[float, float]] | tuple[float, float]:
    if isiterable(c):
        return [(v.ra.degree, v.dec.degree) for v in c]
    else:
        return (c.ra.degree, c.dec.degree)


AngleSerializable = Annotated[
    Angle, PlainSerializer(lambda x: x.degree, return_type=float)
]
"""Angle with serialization support."""

SkyCoordSerializable = Annotated[
    SkyCoord, PlainSerializer(_serialize_sky_coord)
]
"""Sky coordinate with serialization support."""

Range: TypeAlias = tuple[float, float]
"""Type representing a range of a coordinate."""


class Stencil(BaseModel, ABC):
    """Base class for a stencil parameter."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    @abstractmethod
    def from_string(cls, params: str) -> Self:
        """Parse a string representation of stencil parameters to an object."""


class CutoutRequest(BaseModel):
    """Data for a single cutout request."""

    dataset_ids: list[str] = Field(
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

    @model_validator(mode="after")
    def _validate_stencils(self) -> Self:
        for stencil in self.stencils:
            if isinstance(stencil, RangeStencil):
                raise ValueError("RANGE stencils are not supported")
        return self


class CircleStencil(Stencil):
    """Represents a ``CIRCLE`` or ``POS=CIRCLE`` stencil."""

    type: Literal["circle"] = "circle"

    center: SkyCoordSerializable = Field(
        ..., title="Center", description="Center of the circle"
    )

    radius: AngleSerializable = Field(
        ..., title="Radius", description="Radius of the circle"
    )

    @classmethod
    def from_string(cls, params: str) -> Self:
        ra, dec, radius = (float(p) for p in params.split())
        return cls(
            center=SkyCoord(ra * u.degree, dec * u.degree, frame="icrs"),
            radius=Angle(radius * u.degree),
        )


class PolygonStencil(Stencil):
    """Represents a ``POLYGON`` or ``POS=POLYGON`` stencil."""

    type: Literal["polygon"] = "polygon"

    vertices: SkyCoordSerializable = Field(
        ...,
        title="Vertices",
        description=(
            "Vertices of the polygon, with counter-clockwise winding when"
            " viewed from the origin towards the sky"
        ),
    )

    @classmethod
    def from_string(cls, params: str) -> Self:
        data = [float(p) for p in params.split()]
        if len(data) % 2 != 0:
            msg = f"Odd number of coordinates in vertex list {params}"
            raise ValueError(msg)
        if len(data) < 6:
            msg = "Polygons require at least three vertices"
            raise ValueError(msg)
        ras = []
        decs = []
        for i in range(0, len(data), 2):
            ras.append(data[i])
            decs.append(data[i + 1])
        vertices = SkyCoord(ras * u.degree, decs * u.degree, frame="icrs")
        return cls(vertices=vertices)


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
            ra=(ra_min, ra_max),
            dec=(dec_min, dec_max),
        )
