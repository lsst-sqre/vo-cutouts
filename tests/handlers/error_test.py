"""Test that errors produce a proper backtrace."""

from __future__ import annotations

from unittest.mock import ANY

import pytest
from httpx import AsyncClient
from safir.database import create_database_engine
from safir.testing.slack import MockSlackWebhook
from sqlalchemy.exc import ProgrammingError

from vocutouts.config import config
from vocutouts.uws.schema import Base


@pytest.mark.asyncio
async def test_uncaught_error(
    client: AsyncClient, mock_slack: MockSlackWebhook
) -> None:
    """Check that uncaught errors are reported to Slack.

    Drop the schema, which will ensure that all database errors will throw a
    SQLAlchemy exception. Previously this would result in a 500 error with no
    meaningful information and no exception traceback due a bug in swallowing
    errors in subapps.
    """
    engine = create_database_engine(
        str(config.database_url), config.database_password
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    # Now try to start a job, which should throw a meaningful exception.
    with pytest.raises(ProgrammingError):
        await client.get(
            "/api/cutout/sync",
            headers={"X-Auth-Request-User": "someone"},
            params={"ID": "1:2:band:id", "Pos": "CIRCLE 0 -2 2"},
        )

    # Ensure that this message is reported to Slack.
    assert mock_slack.messages == [
        {
            "blocks": [
                {
                    "text": {
                        "text": "Uncaught exception in vo-cutouts",
                        "type": "mrkdwn",
                        "verbatim": True,
                    },
                    "type": "section",
                },
                {
                    "fields": [
                        {
                            "text": "*Exception type*\nProgrammingError",
                            "type": "mrkdwn",
                            "verbatim": True,
                        },
                        {
                            "text": ANY,
                            "type": "mrkdwn",
                            "verbatim": True,
                        },
                    ],
                    "type": "section",
                },
                {
                    "text": {
                        "text": ANY,
                        "type": "mrkdwn",
                        "verbatim": True,
                    },
                    "type": "section",
                },
                {
                    "type": "divider",
                },
            ],
        },
    ]
