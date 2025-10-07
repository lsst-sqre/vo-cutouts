"""Domain model for a cutout request, suitable for passing to the worker.

Note
----
This module must not depend on any other portions of the vo-cutouts
application or on the UWS library apart from the library for UWS backend
workers in order to force too many dependencies on the backend worker. It
should try to use only dependencies present in the stack container.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from astropy import units as u
from astropy.coordinates import Angle, SkyCoord
from astropy.utils import isiterable
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
)

__all__ = [
    "AngleSerializable",
    "SkyCoordSerializable",
    "WorkerCircleStencil",
    "WorkerCutout",
    "WorkerPolygonStencil",
    "WorkerRange",
    "WorkerRangeStencil",
    "WorkerStencil",
]


def _deserialize_angle(c: Angle | float) -> Angle:
    return c if isinstance(c, Angle) else Angle(c * u.degree)


def _serialize_sky_coord(
    c: SkyCoord,
) -> list[tuple[float, float]] | tuple[float, float]:
    if isiterable(c):
        return [(float(v.ra.degree), float(v.dec.degree)) for v in c]
    else:
        return (float(c.ra.degree), float(c.dec.degree))


def _deserialize_sky_coord(c: Any) -> SkyCoord:
    if isinstance(c, SkyCoord):
        return c
    if isiterable(c[0]):
        ras = [v[0] for v in c]
        decs = [v[1] for v in c]
        return SkyCoord(ras * u.degree, decs * u.degree, frame="icrs")
    else:
        return SkyCoord(c[0] * u.degree, c[1] * u.degree, frame="icrs")


type AngleSerializable = Annotated[
    Angle,
    BeforeValidator(_deserialize_angle),
    PlainSerializer(lambda x: float(x.degree), return_type=float),
]
"""Angle with serialization support."""

type SkyCoordSerializable = Annotated[
    SkyCoord,
    BeforeValidator(_deserialize_sky_coord),
    PlainSerializer(_serialize_sky_coord),
]
"""Sky coordinate with serialization support."""

type WorkerRange = tuple[float, float]
"""Type representing a range of a coordinate."""


class WorkerCircleStencil(BaseModel):
    """Represents a ``CIRCLE`` or ``POS=CIRCLE`` stencil."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    type: Literal["circle"] = "circle"

    center: SkyCoordSerializable
    """Center of the circle."""

    radius: AngleSerializable
    """Radius of the circle."""


class WorkerPolygonStencil(BaseModel):
    """Represents a ``POLYGON`` or ``POS=POLYGON`` stencil."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    type: Literal["polygon"] = "polygon"

    vertices: SkyCoordSerializable
    """Vertices of the polygon in counter-clockwise winding."""


class WorkerRangeStencil(BaseModel):
    """Represents a ``POS=RANGE`` stencil."""

    type: Literal["range"] = "range"

    ra: WorkerRange
    """Range of ra values, using inf or -inf for open ranges."""

    dec: WorkerRange
    """Range of dec values, using inf or -info for open ranges."""


type WorkerStencil = Annotated[
    WorkerCircleStencil | WorkerPolygonStencil | WorkerRangeStencil,
    Field(discriminator="type"),
]
"""An instance of any supported stencil."""


class WorkerCutout(BaseModel):
    """Data for a single cutout request."""

    dataset_ids: list[str]
    """Dataset IDs from which to generate a cutout."""

    stencils: list[WorkerStencil]
    """Stencils for the cutouts."""

    cutout_mode: Literal["image", "masked_image", "exposure"] = "image"
    """Cutout mode. Only the image pixels, the image, variance, and mask, or
    the full original exposure.
    """
