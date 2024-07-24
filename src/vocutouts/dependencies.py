"""Job parameter dependencies."""

from typing import Annotated

from fastapi import Depends, Form, Query, Request
from safir.uws import UWSJobParameter, uws_post_params_dependency

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
        UWSJobParameter(parameter_id=k.lower(), value=v)
        for k, v in request.query_params.items()
        if k in {"id", "pos", "circle", "polygon"}
    ]


async def post_params_dependency(
    *,
    id: Annotated[
        str | list[str] | None,
        Form(
            title="Source ID",
            description=(
                "Identifiers of images from which to make a cutout. This"
                " parameter is mandatory."
            ),
        ),
    ] = None,
    pos: Annotated[
        str | list[str] | None,
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
        str | list[str] | None,
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
        str | list[str] | None,
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
    params: Annotated[
        list[UWSJobParameter], Depends(uws_post_params_dependency)
    ],
) -> list[UWSJobParameter]:
    """Parse POST parameters into job parameters for a cutout."""
    return [
        p
        for p in params
        if p.parameter_id in {"id", "pos", "circle", "polygon"}
    ]
