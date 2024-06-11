"""Tests for cutout parameter models."""

from __future__ import annotations

import pickle

from vocutouts.models.parameters import CutoutParameters
from vocutouts.models.stencils import (
    CircleStencil,
    PolygonStencil,
    RangeStencil,
)


def test_pickle() -> None:
    params = CutoutParameters(
        ids=["foo", "bar"],
        stencils=[
            CircleStencil.from_string("1 1.42 1"),
            PolygonStencil.from_string("1 0 1 1 0 1 0 0"),
            RangeStencil.from_string("-Inf 1 0 1"),
        ],
    )
    params_pickle = pickle.loads(pickle.dumps(params))
    assert params.ids == params_pickle.ids
    expected = [s.to_dict() for s in params.stencils]
    seen = [s.to_dict() for s in params_pickle.stencils]
    assert expected == seen
