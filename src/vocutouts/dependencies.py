"""Job parameter dependencies."""

from typing import Annotated

from fastapi import Form, Query, Request
from safir.uws import UWSJobParameter

__all__ = [
    "get_params_dependency",
    "post_params_dependency",
]


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
    request: Request,
) -> list[UWSJobParameter]:
    """Parse GET parameters into job parameters for a cutout."""
    return [
        UWSJobParameter(parameter_id=k, value=v)
        for k, v in request.query_params.multi_items()
        if k in {"id", "pos", "circle", "polygon"}
    ]


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
) -> list[UWSJobParameter]:
    """Parse POST parameters into job parameters for a cutout."""
    params: list[UWSJobParameter] = []
    for name, values in (
        ("id", id),
        ("pos", pos),
        ("circle", circle),
        ("polygon", polygon),
    ):
        params.extend(
            UWSJobParameter(parameter_id=name, value=v) for v in values or []
        )
    return params
