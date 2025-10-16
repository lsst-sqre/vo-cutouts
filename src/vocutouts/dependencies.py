"""Job parameter dependencies."""

from itertools import chain
from typing import Annotated, Literal

from fastapi import Form, Query
from pydantic import ValidationError
from safir.uws import ParameterError

from .exceptions import MultiValuedParameterError
from .models.cutout import (
    CircleStencil,
    CutoutParameters,
    PolygonStencil,
    StencilType,
)

__all__ = [
    "get_params_dependency",
    "post_params_dependency",
]


def _build_parameters(
    *,
    ids: list[str],
    mode: Literal["image", "masked-image", "exposure"],
    pos: list[str],
    circle: list[str],
    polygon: list[str],
) -> CutoutParameters:
    """Construct the Pydantic model from query or form parameters.

    Parameters
    ----------
    ids
        Identifiers of images from which to make a cuotut.
    mode
        Cutout mode.
    pos
        POS-type stencils.
    circle
        CIRCLE-type stencils.
    polygon
        POLYGON-type stencils.

    Returns
    -------
    CutoutParameters
        Corresponding Pydantic model.

    Raises
    ------
    MultiValuedParameterError
        Raised if multiple IDs or multiple stencils are provided, since those
        currently aren't supported.
    ParameterError
        Raised if one of the parameters is invalid and cannot be parsed.
    """
    pos_iter = (s.split(None, 1) for s in pos)
    circle_iter = (("CIRCLE", s) for s in circle)
    polygon_iter = (("POLYGON", s) for s in polygon)
    stencils: list[StencilType] = []

    # Range stencils are not currently supported by the backend and are
    # therefore intentionally left out of this match.
    for stencil_type, params in chain(pos_iter, circle_iter, polygon_iter):
        try:
            match stencil_type:
                case "CIRCLE":
                    stencils.append(CircleStencil.from_string(params))
                case "POLYGON":
                    stencils.append(PolygonStencil.from_string(params))
                case _:
                    msg = f"Unknown stencil type {stencil_type}"
                    raise ParameterError(msg)
        except Exception as e:
            msg = f"Invalid cutout parameter: {stencil_type} {params}: {e!s}"
            raise ParameterError(msg) from e

    # For now, only support a single ID and stencil. These have to be
    # checked outside of the validator because the SODA standard requires
    # returning a different error in this case.
    if len(ids) > 1:
        raise MultiValuedParameterError("Only one ID supported")
    if len(stencils) > 1:
        raise MultiValuedParameterError("Only one stencil is supported")

    # Return the parsed parameters.
    try:
        return CutoutParameters(ids=ids, cutout_mode=mode, stencils=stencils)
    except ValidationError as e:
        raise ParameterError("Invalid parameters", str(e)) from e


async def get_params_dependency(
    *,
    id: Annotated[
        list[str],
        Query(
            title="Source ID",
            description=(
                "Identifiers of images from which to make a cutout. This"
                " parameter is mandatory."
            ),
        ),
    ],
    cutoutmode: Annotated[
        Literal["image", "masked-image", "exposure"],
        Query(
            title="Cutout mode",
            description=(
                "Specifies the amount of information to include in the cutout:"
                " only the image pixels; the image, variance, and mask; or"
                " the full original exposure"
            ),
        ),
    ] = "image",
    pos: Annotated[
        list[str] | None,
        Query(
            title="Cutout positions",
            description=(
                "Positions to cut out. Supported parameters are RANGE followed"
                " by min and max ra and min and max dec; CIRCLE followed by"
                " ra, dec, and radius; and POLYGON followed by a list of"
                " ra/dec positions for vertices. Arguments must be separated"
                " by spaces and parameters are double-precision floating point"
                " numbers expressed as strings."
            ),
        ),
    ] = None,
    circle: Annotated[
        list[str] | None,
        Query(
            title="Cutout circle positions",
            description=(
                "Circles to cut out. The value must be the ra and dec of the"
                " center of the circle and then the radius, as"
                " double-precision floating point numbers expressed as"
                " strings and separated by spaces."
            ),
        ),
    ] = None,
    polygon: Annotated[
        list[str] | None,
        Query(
            title="Cutout polygon positions",
            description=(
                "Polygons to cut out. The value must be ra/dec pairs for each"
                " vertex, ordered so that the polygon winding direction is"
                " counter-clockwise (when viewed from the origin towards the"
                " sky). These parameters are double-precision floating point"
                " numbers expressed as strings and separated by spaces."
            ),
        ),
    ] = None,
) -> CutoutParameters:
    """Parse GET parameters into job parameters for a cutout."""
    return _build_parameters(
        ids=id,
        mode=cutoutmode,
        pos=pos or [],
        circle=circle or [],
        polygon=polygon or [],
    )


async def post_params_dependency(
    *,
    id: Annotated[
        list[str],
        Form(
            title="Source ID",
            description=(
                "Identifiers of images from which to make a cutout. This"
                " parameter is mandatory."
            ),
        ),
    ],
    cutoutmode: Annotated[
        Literal["image", "masked-image", "exposure"],
        Form(
            title="Cutout mode",
            description=(
                "Specifies the amount of information to include in the cutout:"
                " only the image pixels; the image, variance, and mask; or"
                " the full original exposure"
            ),
        ),
    ] = "image",
    pos: Annotated[
        list[str] | None,
        Form(
            title="Cutout positions",
            description=(
                "Positions to cut out. Supported parameters are RANGE followed"
                " by min and max ra and min and max dec; CIRCLE followed by"
                " ra, dec, and radius; and POLYGON followed by a list of"
                " ra/dec positions for vertices. Arguments must be separated"
                " by spaces and parameters are double-precision floating point"
                " numbers expressed as strings."
            ),
        ),
    ] = None,
    circle: Annotated[
        list[str] | None,
        Form(
            title="Cutout circle positions",
            description=(
                "Circles to cut out. The value must be the ra and dec of the"
                " center of the circle and then the radius, as"
                " double-precision floating point numbers expressed as"
                " strings and separated by spaces."
            ),
        ),
    ] = None,
    polygon: Annotated[
        list[str] | None,
        Form(
            title="Cutout polygon positions",
            description=(
                "Polygons to cut out. The value must be ra/dec pairs for each"
                " vertex, ordered so that the polygon winding direction is"
                " counter-clockwise (when viewed from the origin towards the"
                " sky). These parameters are double-precision floating point"
                " numbers expressed as strings and separated by spaces."
            ),
        ),
    ] = None,
) -> CutoutParameters:
    """Parse POST parameters into job parameters for a cutout."""
    return _build_parameters(
        ids=id,
        mode=cutoutmode,
        pos=pos or [],
        circle=circle or [],
        polygon=polygon or [],
    )
