#!/bin/bash
#
# Start the vo-cutouts frontend application.  Currently creates the database
# and then starts the server.  Eventually, this will call Alembic to handle
# database migrations.

set -eu

vo-cutouts init
uvicorn vocutouts.main:app --host 0.0.0.0 --port 8080
