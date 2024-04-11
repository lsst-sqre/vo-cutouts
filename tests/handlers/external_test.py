"""Catch-all tests for miscellaneous external routes."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from vocutouts.config import config

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
      <accessURL use="full">https://example.com/api/cutout/capabilities\
</accessURL>
    </interface>
  </capability>
  <capability standardID="ivo://ivoa.net/std/VOSI#availability">
    <interface xsi:type="vod:ParamHTTP" version="1.0">
      <accessURL use="full">https://example.com/api/cutout/availability\
</accessURL>
    </interface>
  </capability>
  <capability standardid="ivo://ivoa.net/std/SODA#sync-1.0">
    <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
      <accessURL use="full">https://example.com/api/cutout/sync</accessURL>
    </interface>
  </capability>
  <capability standardid="ivo://ivoa.net/std/SODA#async-1.0">
    <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
      <accessURL use="full">https://example.com/api/cutout/jobs</accessURL>
    </interface>
  </capability>
</capabilities>
"""


@pytest.mark.asyncio
async def test_get_index(client: AsyncClient) -> None:
    """Test ``GET /api/cutout/``."""
    response = await client.get("/api/cutout/")
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
    r = await client.get("/api/cutout/availability")
    assert r.status_code == 200
    assert r.text == AVAILABILITY.strip()


@pytest.mark.asyncio
async def test_capabilities(client: AsyncClient) -> None:
    r = await client.get("/api/cutout/capabilities")
    assert r.status_code == 200
    assert r.text == CAPABILITIES.strip()


@pytest.mark.asyncio
async def test_capabilities_urls(app: FastAPI) -> None:
    """Test the scheme in the URLs for the capabilities endpoint.

    When running in a Kubernetes cluster behind an ingress that terminates
    TLS, the request as seen by the application will be ``http``, but we want
    the generated URLs to honor ``X-Forwarded-Proto`` and thus use ``https``.
    We also want to honor the ``Host`` header.
    """
    transport = ASGITransport(app=app)  # type: ignore[arg-type]
    async with AsyncClient(
        transport=transport, base_url="http://foo.com/"
    ) as client:
        r = await client.get(
            "/api/cutout/capabilities",
            headers={
                "Host": "example.com",
                "X-Forwarded-For": "10.10.10.10",
                "X-Forwarded-Proto": "https",
                "X-Forwarded-Host": "foo.com",
            },
        )
        assert r.status_code == 200
        assert r.text == CAPABILITIES.strip()
