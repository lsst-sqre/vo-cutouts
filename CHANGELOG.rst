##########
Change log
##########

0.2.0 (2022-02-09)
==================

This is the initial production candidate.
Another release will be forthcoming to clean up some remaining issues, but this version contains the core functionality and uses a proper backend.

The database schema of this version is incompatible with 0.1.0.
The database must be wiped and recreated during the upgrade.

- Use ``lsst.image_cutout_backend`` as the backend instead of ``pipetask`` without conversion of coordinates to pixels.
- Data IDs are now Butler UUIDs instead of colon-separated tuples.
- Support POLYGON and CIRCLE stencils and stop supporting POS RANGE, matching the capabilities of the new backend.
- Use a separate S3 bucket to store the output rather than a Butler collection.
  Eliminate use of Butler in the frontend, in favor of using that S3 bucket directly.
  This eliminated the ``CUTOUT_BUTLER_COLLECTION`` configuration setting and adds new ``CUTOUT_STORAGE_URL`` and ``CUTOUT_TMPDIR`` configuration settings.
- Use a different method of generating signed S3 result URLs that works correctly with workload identity in a GKE cluster.
  This adds a new ``CUTOUT_SERVICE_ACCOUNT`` configuration setting specifying the service account to use for URL signing.
  The workload identity the service runs as must have the ``roles/iam.serviceAccountTokenCreator`` role so that it can create signed URLs.
- Add new ``--reset`` flag to ``vo-cutouts init`` to wipe the existing database.
- Stop using a FastAPI subapp.
  This was causing problems for error handling, leading to exceptions thrown in the UWS handlers to turn into 500 errors with no logged exception and no error details.

0.1.0 (2021-11-11)
==================

Initial version, which uses a colon-separated tuple as the ``ID`` parameter and has an initial proof-of-concept backend that runs ``pipetask`` via ``subprocess`` and does not do correct conversion of coordinates to pixels.

This is only a proof of concept release.
Some of the things it does are very slow and block the whole asyncio process.
The backend will be changed significantly before the first production release.
