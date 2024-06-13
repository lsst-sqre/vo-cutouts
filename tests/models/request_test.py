"""Tests for cutout parameter models."""

from __future__ import annotations

import pickle

from vocutouts.models.request import (
    CircleStencil,
    CutoutRequest,
    PolygonStencil,
)


def test_pickle() -> None:
    for params in (
        CutoutRequest(
            dataset_ids=["foo"],
            stencils=[CircleStencil.from_string("1 1.42 1")],
        ),
        CutoutRequest(
            dataset_ids=["foo"],
            stencils=[PolygonStencil.from_string("1 0 1 1 0 1 0 0")],
        ),
    ):
        params_pickle = pickle.loads(pickle.dumps(params))
        assert params.dataset_ids == params_pickle.dataset_ids
        expected = [s.model_dump(mode="json") for s in params.stencils]
        seen = [s.model_dump(mode="json") for s in params_pickle.stencils]
        assert expected == seen
