"""Tests for cutout parameter models."""

from __future__ import annotations

import math
import pickle

from vocutouts.models.cutout import (
    CircleStencil,
    CutoutParameters,
    PolygonStencil,
    RangeStencil,
)
from vocutouts.models.domain.cutout import WorkerCutout, WorkerPolygonStencil


def test_pickle() -> None:
    for cutout in (
        CutoutParameters(
            ids=["foo"],
            stencils=[CircleStencil.from_string("1 1.42 1")],
        ).to_worker_parameters(),
        CutoutParameters(
            ids=["foo"],
            stencils=[PolygonStencil.from_string("1 0 1 1 0 1 0 0")],
        ).to_worker_parameters(),
    ):
        cutout_pickle = pickle.loads(pickle.dumps(cutout))
        assert cutout.dataset_ids == cutout_pickle.dataset_ids
        expected = [s.model_dump(mode="json") for s in cutout.stencils]
        seen = [s.model_dump(mode="json") for s in cutout_pickle.stencils]
        assert expected == seen


def test_serialize() -> None:
    cutout = CutoutParameters(
        ids=["foo"],
        stencils=[CircleStencil.from_string("1 1.42 1")],
    ).to_worker_parameters()
    assert cutout.model_dump() == {
        "dataset_ids": ["foo"],
        "stencils": [
            {
                "type": "circle",
                "center": (1.0, 1.42),
                "radius": 1.0,
            }
        ],
    }
    assert cutout == WorkerCutout.model_validate(cutout.model_dump())

    cutout = CutoutParameters(
        ids=["foo"],
        stencils=[PolygonStencil.from_string("1.2 0 1 1.4 0 1 0 0.5")],
    ).to_worker_parameters()
    vertices = [(1.2, 0.0), (1.0, 1.4), (0.0, 1.0), (0.0, 0.5)]
    assert cutout.model_dump() == {
        "dataset_ids": ["foo"],
        "stencils": [
            {
                "type": "polygon",
                "vertices": vertices,
            }
        ],
    }

    # A SkyCoord with multiple coordinates cannot be compared with Python
    # equality, so we have to do this the hard way.
    serialized_cutout = WorkerCutout.model_validate(cutout.model_dump())
    assert cutout.dataset_ids == serialized_cutout.dataset_ids
    assert len(serialized_cutout.stencils) == 1
    assert isinstance(serialized_cutout.stencils[0], WorkerPolygonStencil)
    assert vertices == [
        (float(v.ra.degree), float(v.dec.degree))
        for v in serialized_cutout.stencils[0].vertices
    ]

    cutout = CutoutParameters(
        ids=["foo"],
        stencils=[RangeStencil.from_string("1 inf -inf 0")],
    ).to_worker_parameters()
    assert cutout.model_dump() == {
        "dataset_ids": ["foo"],
        "stencils": [
            {
                "type": "range",
                "ra": (1.0, math.inf),
                "dec": (-math.inf, 0.0),
            }
        ],
    }
    assert cutout == WorkerCutout.model_validate(cutout.model_dump())
