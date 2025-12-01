"""Representation of request parameters.

This is the model used by the user-facing API. These models are converted into
domain models for passing to the backend.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from itertools import batched
from typing import Annotated, Literal, Self, override

from astropy import units as u
from astropy.coordinates import Angle, SkyCoord
from pydantic import BaseModel, Field
from safir.uws import ParametersModel
from vo_models.uws import MultiValuedParameter, Parameter, Parameters

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
    "CutoutXmlParameters",
    "Point",
    "PolygonStencil",
    "Range",
    "RangeStencil",
    "Stencil",
    "StencilType",
]


class CutoutXmlParameters(Parameters):
    """XML representation of cutout parameters."""

    id: MultiValuedParameter
    cutoutdetail: Parameter = Parameter(id="cutoutdetail")
    pos: MultiValuedParameter = Field([])
    circle: MultiValuedParameter = Field([])
    polygon: MultiValuedParameter = Field([])


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
    def to_string(self) -> str:
        """Convert to the string parameter representation."""

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

    @override
    @classmethod
    def from_string(cls, params: str) -> Self:
        ra, dec, radius = (float(p) for p in params.split())
        return cls(center=Point(ra=ra, dec=dec), radius=radius)

    @override
    def to_string(self) -> str:
        return f"{self.center.ra!s} {self.center.dec!s} {self.radius!s}"

    @override
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
        min_length=3,
    )

    @override
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

    @override
    def to_string(self) -> str:
        return " ".join(f"{v.ra!s} {v.dec!s}" for v in self.vertices)

    @override
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

    @override
    @classmethod
    def from_string(cls, params: str) -> Self:
        ra_min, ra_max, dec_min, dec_max = (float(p) for p in params.split())
        return cls(
            ra=Range(min=ra_min, max=ra_max),
            dec=Range(min=dec_min, max=dec_max),
        )

    @override
    def to_string(self) -> str:
        return (
            f"{self.ra.min!s} {self.ra.max!s}"
            f" {self.dec.min!s} {self.dec.max!s}"
        )

    @override
    def to_worker_stencil(self) -> WorkerStencil:
        return WorkerRangeStencil(
            ra=(self.ra.min, self.ra.max),
            dec=(self.dec.min, self.dec.max),
        )


type StencilType = Annotated[
    CircleStencil | PolygonStencil | RangeStencil, Field(discriminator="type")
]
"""Type for any stencil, concrete due to Pydantic requirements."""


class CutoutParameters(ParametersModel[WorkerCutout, CutoutXmlParameters]):
    """Parameters to a cutout request."""

    ids: Annotated[
        list[str],
        Field(
            title="Dataset IDs",
            description=(
                "Dataset IDs on which to operate. Currently, only one dataset"
                " ID is permitted."
            ),
            min_length=1,
            max_length=1,
        ),
    ]

    stencils: Annotated[
        list[StencilType],
        Field(
            title="Cutout stencils",
            description=(
                "Cutout stencils describing the desired cutouts. Currently,"
                " only one stencil is supported."
            ),
            min_length=1,
            max_length=1,
        ),
    ]

    cutout_detail: Annotated[
        Literal["Image", "MaskedImage", "Exposure"],
        Field(
            title="Detail to include in cutout",
            description=(
                "Amount of information to include in the cutout: Image for"
                " only the image pixels; MaskedImage for the image, variance,"
                " and mask; or Exposure for the full original exposure"
                " including afw-format metadata tables."
            ),
        ),
    ] = "Image"

    @override
    def to_worker_parameters(self) -> WorkerCutout:
        stencils = [s.to_worker_stencil() for s in self.stencils]
        return WorkerCutout(
            dataset_ids=self.ids,
            cutout_detail=self.cutout_detail,
            stencils=stencils,
        )

    @override
    def to_xml_model(self) -> CutoutXmlParameters:
        circle = []
        polygon = []
        pos = []
        for stencil in self.stencils:
            match stencil:
                case CircleStencil():
                    value = stencil.to_string()
                    circle.append(Parameter(id="circle", value=value))
                case PolygonStencil():
                    value = stencil.to_string()
                    polygon.append(Parameter(id="polygon", value=value))
                case RangeStencil():
                    value = "RANGE " + stencil.to_string()
                    pos.append(Parameter(id="pos", value=value))
                case _:
                    raise ValueError(f"Unknown stencil type {stencil.type}")
        return CutoutXmlParameters(
            id=[Parameter(id="id", value=i) for i in self.ids],
            cutoutdetail=Parameter(
                id="cutoutdetail", value=self.cutout_detail
            ),
            circle=circle,
            polygon=polygon,
            pos=pos,
        )
