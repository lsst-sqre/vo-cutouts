"""Tests for cutout parameter models."""

from __future__ import annotations

import math

from vocutouts.models.cutout import (
    CircleStencil,
    CutoutParameters,
    PolygonStencil,
    RangeStencil,
)
from vocutouts.models.domain.cutout import WorkerCutout, WorkerPolygonStencil


def test_serialize() -> None:
    cutout = CutoutParameters(
        ids=["foo"],
        stencils=[CircleStencil.from_string("1 1.42 1")],
    ).to_worker_parameters()
    serialized = cutout.model_dump(mode="json")
    assert serialized == {
        "dataset_ids": ["foo"],
        "cutout_detail": "Image",
        "stencils": [
            {
                "type": "circle",
                "center": [1.0, 1.42],
                "radius": 1.0,
            }
        ],
    }
    assert cutout == WorkerCutout.model_validate(serialized)

    cutout = CutoutParameters(
        ids=["foo"],
        stencils=[PolygonStencil.from_string("1.2 0 1 1.4 0 1 0 0.5")],
    ).to_worker_parameters()
    vertices = [[1.2, 0.0], [1.0, 1.4], [0.0, 1.0], [0.0, 0.5]]
    serialized = cutout.model_dump(mode="json")
    assert serialized == {
        "dataset_ids": ["foo"],
        "cutout_detail": "Image",
        "stencils": [
            {
                "type": "polygon",
                "vertices": vertices,
            }
        ],
    }

    # A SkyCoord with multiple coordinates cannot be compared with Python
    # equality, so we have to do this the hard way.
    unserialized_cutout = WorkerCutout.model_validate(serialized)
    assert cutout.dataset_ids == unserialized_cutout.dataset_ids
    assert len(unserialized_cutout.stencils) == 1
    assert isinstance(unserialized_cutout.stencils[0], WorkerPolygonStencil)
    assert vertices == [
        [float(v.ra.degree), float(v.dec.degree)]
        for v in unserialized_cutout.stencils[0].vertices
    ]

    cutout = CutoutParameters(
        ids=["foo"],
        stencils=[RangeStencil.from_string("1 inf -inf 0")],
    ).to_worker_parameters()
    serialized = cutout.model_dump(mode="json")
    assert serialized == {
        "dataset_ids": ["foo"],
        "cutout_detail": "Image",
        "stencils": [
            {
                "type": "range",
                "ra": [1.0, math.inf],
                "dec": [-math.inf, 0.0],
            }
        ],
    }
    assert cutout == WorkerCutout.model_validate(serialized)
