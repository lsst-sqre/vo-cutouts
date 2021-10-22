"""Catch-all tests for miscellaneous external routes."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from vocutouts.config import config

if TYPE_CHECKING:
    from httpx import AsyncClient

AVAILABILITY = """
<vosi:availability xmlns:vosi="http://www.ivoa.net/xml/VOSIAvailability/v1.0">
  <vosi:available>true</vosi:available>
</vosi:availability>
"""

CAPABILITIES = """
<?xml version="1.0"?>
<capabilities
    xmlns:vosi="http://www.ivoa.net/xml/VOSICapabilities/v1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:vod="http://www.ivoa.net/xml/VODataService/v1.1">
  <capability standardID="ivo://ivoa.net/std/VOSI#capabilities">
    <interface xsi:type="vod:ParamHTTP" version="1.0">
      <accessURL use="full">https://example.com/cutout/capabilities</accessURL>
    </interface>
  </capability>
  <capability standardID="ivo://ivoa.net/std/VOSI#availability">
    <interface xsi:type="vod:ParamHTTP" version="1.0">
      <accessURL use="full">https://example.com/cutout/availability</accessURL>
    </interface>
  </capability>
  <capability standardid="ivo://ivoa.net/std/SODA#sync-1.0">
    <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
      <accessURL use="full">https://example.com/cutout/sync</accessURL>
    </interface>
  </capability>
  <capability standardid="ivo://ivoa.net/std/SODA#async-1.0">
    <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
      <accessURL use="full">https://example.com/cutout/jobs</accessURL>
    </interface>
  </capability>
</capabilities>
"""


@pytest.mark.asyncio
async def test_get_index(client: AsyncClient) -> None:
    """Test ``GET /cutout/``"""
    response = await client.get("/cutout/")
    assert response.status_code == 200
    data = response.json()
    metadata = data["metadata"]
    assert metadata["name"] == config.name
    assert isinstance(metadata["version"], str)
    assert isinstance(metadata["description"], str)
    assert isinstance(metadata["repository_url"], str)
    assert isinstance(metadata["documentation_url"], str)


@pytest.mark.asyncio
async def test_availability(client: AsyncClient) -> None:
    r = await client.get("/cutout/availability")
    assert r.status_code == 200
    assert r.text == AVAILABILITY.strip()


@pytest.mark.asyncio
async def test_capabilities(client: AsyncClient) -> None:
    r = await client.get("/cutout/capabilities")
    assert r.status_code == 200
    assert r.text == CAPABILITIES.strip()
