"""Parsing and representation of stencil parameters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Tuple

from astropy import units as u
from astropy.coordinates import Angle, SkyCoord

Range = Tuple[float, float]


class Stencil(ABC):
    """Base class for a stencil parameter."""

    @classmethod
    @abstractmethod
    def from_string(cls, params: str) -> Stencil:
        """Parse a string representation of stencil parameters to an object."""

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert the stencil to a JSON-serializable form for queuing."""


@dataclass
class CircleStencil(Stencil):
    """Represents a ``CIRCLE`` or ``POS=CIRCLE`` stencil."""

    center: SkyCoord
    radius: Angle

    @classmethod
    def from_string(cls, params: str) -> CircleStencil:
        ra, dec, radius = (float(p) for p in params.split())
        return cls(
            center=SkyCoord(ra * u.degree, dec * u.degree, frame="icrs"),
            radius=Angle(radius * u.degree),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "circle",
            "center": {
                "ra": self.center.ra.degree,
                "dec": self.center.dec.degree,
            },
            "radius": self.radius.degree,
        }


@dataclass
class PolygonStencil(Stencil):
    """Represents a ``POLYGON`` or ``POS=POLYGON`` stencil.

    Represents the polygon defined by the given vertices.  Polygon winding
    must be counter-clockwise when viewed from the origin towards the sky.
    """

    vertices: SkyCoord

    @classmethod
    def from_string(cls, params: str) -> PolygonStencil:
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

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "polygon",
            "vertices": [(v.ra.degree, v.dec.degree) for v in self.vertices],
        }


@dataclass
class RangeStencil(Stencil):
    """Represents a ``POS=RANGE`` stencil."""

    ra: Range
    dec: Range

    @classmethod
    def from_string(cls, params: str) -> RangeStencil:
        ra_min, ra_max, dec_min, dec_max = (float(p) for p in params.split())
        return cls(
            ra=(ra_min, ra_max),
            dec=(dec_min, dec_max),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "range",
            "ra": self.ra,
            "dec": self.dec,
        }


def parse_stencil(stencil_type: str, params: str) -> Stencil:
    """Convert a string stencil parameter to its internal representation."""
    if stencil_type == "POS":
        stencil_type, params = params.split(None, 1)
    if stencil_type == "CIRCLE":
        return CircleStencil.from_string(params)
    elif stencil_type == "POLYGON":
        return PolygonStencil.from_string(params)
    elif stencil_type == "RANGE":
        return RangeStencil.from_string(params)
    else:
        raise ValueError(f"Unknown stencil type {stencil_type}")
