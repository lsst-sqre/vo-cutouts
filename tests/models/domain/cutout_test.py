"""Tests for cutout parameter models."""

from __future__ import annotations

import pickle

from vocutouts.models.cutout import (
    CircleStencil,
    CutoutParameters,
    PolygonStencil,
)


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
