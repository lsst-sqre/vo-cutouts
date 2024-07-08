"""Domain model for a cutout request, suitable for passing to the worker.

Note
----
This module must not depend on any other portions of the vo-cutouts
application or on the UWS library apart from the library for UWS backend
workers in order to force too many dependencies on the backend worker. It
should try to use only dependencies present in the stack container.
"""

from __future__ import annotations

from abc import ABC
from typing import Annotated, Literal, TypeAlias

from astropy.coordinates import Angle, SkyCoord
from astropy.utils import isiterable
from pydantic import BaseModel, ConfigDict, PlainSerializer, SerializeAsAny

__all__ = [
    "AngleSerializable",
    "SkyCoordSerializable",
    "WorkerCutout",
    "WorkerCircleStencil",
    "WorkerPolygonStencil",
    "WorkerRange",
    "WorkerRangeStencil",
    "WorkerStencil",
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

WorkerRange: TypeAlias = tuple[float, float]
"""Type representing a range of a coordinate."""


class WorkerStencil(BaseModel, ABC):
    """Base class for a stencil parameter."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    type: str
    """Type of stencil."""


class WorkerCircleStencil(WorkerStencil):
    """Represents a ``CIRCLE`` or ``POS=CIRCLE`` stencil."""

    type: Literal["circle"] = "circle"

    center: SkyCoordSerializable
    """Center of the circle."""

    radius: AngleSerializable
    """Radius of the circle."""


class WorkerPolygonStencil(WorkerStencil):
    """Represents a ``POLYGON`` or ``POS=POLYGON`` stencil."""

    type: Literal["polygon"] = "polygon"

    vertices: SkyCoordSerializable
    """Vertices of the polygon in counter-clockwise winding."""


class WorkerRangeStencil(WorkerStencil):
    """Represents a ``POS=RANGE`` stencil."""

    type: Literal["range"] = "range"

    ra: WorkerRange
    """Range of ra values, using inf or -inf for open ranges."""

    dec: WorkerRange
    """Range of dec values, using inf or -info for open ranges."""


class WorkerCutout(BaseModel):
    """Data for a single cutout request."""

    dataset_ids: list[str]
    """Dataset IDs from which to generate a cutout."""

    stencils: SerializeAsAny[list[WorkerStencil]]
    """Stencils for the cutouts."""
