# Change log

vo-cutouts is versioned with [semver](https://semver.org/). Dependencies are updated to the latest available version during each release, and aren't noted here.

Find changes for the upcoming release in the project's [changelog.d directory](https://github.com/lsst-sqre/vo-cutouts/tree/main/changelog.d/).

<!-- scriv-insert-here -->

<a id='changelog-4.1.1'></a>
## 4.1.1 (2025-06-17)

### Bug fixes

- Update the worker image to the latest Pipelines stack release (`v29_1_0`).

### Other changes

- Use [uv](https://github.com/astral-sh/uv) to maintain frozen dependencies and set up a development environment.

<a id='changelog-4.1.0'></a>
## 4.1.0 (2025-02-20)

### New features

- Replace internal Butler URI parsing with calls into the Butler library. This adds support for the new Butler URI format that will be used for future data releases.

### Other changes

- Use the cutout backend code that is integrated into the stack container rather than installing code from Git during container build time.

<a id='changelog-4.0.0'></a>
## 4.0.0 (2024-12-12)

### Backwards-incompatible changes

- Switch to Wobbly for job storage. All previous job history will be lost unless the vo-cutouts database is converted into Wobbly's storage format and inserted into Wobbly's database.

### Bug fixes

- Catch errors from parsing the dataset ID or creating a Butler in the backend worker and report them as proper worker exceptions so that they don't produce uncaught exception errors.
- Append a colon after the error code when reporting UWS errors.

### Other changes

- Render all UWS XML output except for error VOTables using vo-models rather than hand-written XML templates.

<a id='changelog-3.2.0'></a>
## 3.2.0 (2024-09-16)

### New features

- Use Alembic to manage the schema of the UWS database. When upgrading to this version, set `config.updateSchema` to true in the Helm configuration for the first deployment. This release contains no schema changes, but needs to perform a migration to add the Alembic version information. The vo-cutouts components will now refuse to start if the database schema has changed and the database has not yet been migrated.

### Bug fixes

- Restore logging configuration during startup of the backend worker, which re-adds support for the logging profile and log level and optionally configures structlog to use a JSON log format. This does not yet extend to the log messages issued directly by arq.

<a id='changelog-3.1.0'></a>
## 3.1.0 (2024-08-02)

### New features

- The database worker pod now deletes the records for all jobs that have passed their destruction times once per hour.
- Restore support for execution duration and change the default execution duration back to 10 minutes. Use a very ugly hack to enforce a timeout in the backend worker that will hopefully not be too fragile.
- Add support for aborting jobs.
- Re-add the `CUTOUT_TIMEOUT` configuration option to change the default and maximum execution duration for cutout jobs.
- Support pre-signed URLs returned by the backend worker. If the result URL is an `http` or `https` URL, pass it to the client unchanged.
- Abort jobs on deletion or expiration if they are pending, queued, or executing.
- Worker pods now wait for 30 seconds (UWS database workers) or 55 seconds (cutout workers) for jobs to finish on shutdown before cancelling them.

### Bug fixes

- Allow time durations in the configuration to be given in number of seconds as a string, which was accidentally broken in 3.0.0.
- Restore support for automatically starting an async job by setting `phase=RUN` in the POST body. The equivalent query parameter was always supported, but POST body support was accidentally dropped in 3.0.0.
- Add a colon after the error code and before the error message in error replies.
- Stop setting `isPost` when returning UWS parameters. This undocumented field is supposed to only be set if the parameter contains a raw POST value rather than a regular parameter, which is never the case here.

### Other changes

- Stop upgrading the operating system packages in the worker image because the base image is so old that the package repositories no longer exist. This will hopefully be fixed in a future release of the Science Pipelines base image based on AlmaLinux.
- Some XML output from UWS handlers is now handled by [vo-models](https://vo-models.readthedocs.io/latest/) instead of hand-written XML templates. More responses will hopefully be converted in the future.

<a id='changelog-3.0.0'></a>
## 3.0.0 (2024-06-28)

### Backwards-incompatible changes

- Cancelling or aborting jobs is not supported by the combination of arq and sync worker functions. Properly reflect this in job metadata by forcing execution duration to 0 to indicate that no limit is applied. Substantially increase the default arq job timeout since the timeout will be ineffective anyway.
- Drop the `CUTOUT_TIMEOUT` configuration option since we have no way of enforcing a timeout on jobs.
- Upgrade the base image for the backend worker to the latest weekly. This includes a new version of `lsst.daf.butler`, which targets a new version of the Butler server with a backwards-incompatible REST API.

### New features

- Support human-readable `4h30m20s`-style strings for `CUTOUT_LIFETIME` and `CUTOUT_SYNC_TIMEOUT` in addition to numbers of seconds.

### Other changes

- Unknown failures in the worker backend are now recorded as fatal UWS errors rather than transient errors. This is the more conservative choice for unknown exceptions.

<a id='changelog-2.0.0'></a>
## 2.0.0 (2024-06-10)

### Backwards-incompatible changes

- Change the job queuing system from Dramatiq to [arq](https://arq-docs.helpmanual.io/). This change should be transparent to users when creating new jobs, but any in-progress jobs at the time of the upgrade will be orphaned.
- Use workload identity for all authentication when deployed on Google Cloud. Separate service account keys are no longer required or used. The `vo-cutouts` Google service account now requires the `storage.legacyBucketWriter` role in addition to `storage.objectViewer`.

### New features

- Add support for `gs` storage URLs in addition to `s3` storage URLs. When a `gs` storage URL is used, the image cutout backend will use the Google Cloud Storage Python API to store the results instead of boto, which will work correctly with workload identity.
- Catch the error thrown when the cutout has no overlap with the specified image and return a more specific error message to the user.
- Add support for sending Slack notifications for uncaught exceptions in route handlers.
- Add support for sending Slack notifications for unexpected errors when processing cutout jobs.
- If the backend image processing code fails with an exception, include a traceback of that exception in the detail portion of the job error.

### Bug fixes

- Queuing a job for execution in the frontend is now async and will not block the event loop, which may help with performance under load.
- Report fatal (not transient) errors on backend failures. We have no way of knowing whether a failure will go away on retry, so make the conservative assumption that it won't.

<a id='changelog-1.1.1'></a>
## 1.1.1 (2024-04-11)

### Other changes

- Update to the latest weekly as a base image for the cutout worker, which picks up new versions of lsst-resources and the Butler client.

<a id='changelog-1.1.0'></a>
## 1.1.0 (2024-02-19)

### New features

- Add support for querying the Butler server rather than instantiating local Butler instances. To support this, vo-cutouts now requires delegated tokens from Gafaelfawr so that it can make API calls on behalf of the user.
- Send uvicorn logs through structlog for consistent JSON formatting and add context expected by Google Cloud Logging to each log message.

### Other changes

- Standardize the environment variables used for configuration. Rename `SAFIR_` environment variables to `CUTOUT_`, remove `SAFIR_LOG_NAME`, and add `CUTOUT_PATH_PREFIX` to control the API path prefix. This is handled by the Phalanx chart, so should be invisible to users.
- Add a change log maintained using [scriv](https://scriv.readthedocs.io/en/latest/).
- Use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting instead of Black, flake8, and isort.

## 1.0.0 (2023-01-12)

There are no major functionality changes in this release. It updates dependencies, packaging, and coding style, makes more use of Safir utility functions, and bumps the version to 1.0.0 since this is acceptable as a release candidate, even though we hope to add additional functionality later.

## 0.4.2 (2022-07-14)

### Bug fixes

- Clip stencils at the edge of the image instead of raising an error in the backend. Practical experience with the Portal and deeper thought about possible scientific use cases have shown this to be a more practical and user-friendly approach.

## 0.4.1 (2022-06-01)

### Bug fixes

- Stop masking pixels outside the cutout stencil. The current performance of masking is unreasonably slow for ``CIRCLE`` cutouts, and masking isn't required by the SODA standard. We may revisit this later with a faster algorithm.

## 0.4.0 (2022-05-31)

### Backwards-incompatible changes

- Dataset IDs are now Butler URIs instead of just the bare UUID. The ``CUTOUT_BUTLER_REPOSITORY`` configuration setting is no longer used. Instead, the backend maintains one instance of a Butler and corresponding cutout backend per named Butler repository, taken from the first component of the Butler URI.

## 0.3.0 (2022-02-23)

### New features

- Build a Docker image (as ``lsstsqre/vo-cutouts-worker``) for the backend worker, based on a Rubin stack container.

### Bug fixes

- Use ``/api/cutout`` as the prefix for all public routes. This was previously done via a rewrite in the ingress. Making the application's internal understanding of its routes match the exposed user-facing routes simplifies the logic and fixes the URLs shown in the ``/api/cutout/capabilities`` endpoint.
- Record all times in the database to second granularity, rather than storing microseconds for some times and not others.
- Fix retries of async database transactions when the database saw a simultaneous write to the same row from another worker.
- Enable results storage for the cutout worker to suppress a Dramatiq warning.

### Other changes

- Add logging to every state-changing route and for each Dramatiq worker operation.

## 0.2.0 (2022-02-09)

This is the initial production candidate. Another release will be forthcoming to clean up some remaining issues, but this version contains the core functionality and uses a proper backend.

### Backwards-incompatible changes

- The database schema of this version is incompatible with 0.1.0. The database must be wiped and recreated during the upgrade.
- Use ``lsst.image_cutout_backend`` as the backend instead of ``pipetask`` without conversion of coordinates to pixels.
- Dataset IDs are now Butler UUIDs instead of colon-separated tuples.
- Support POLYGON and CIRCLE stencils and stop supporting POS RANGE, matching the capabilities of the new backend.
- Use a separate S3 bucket to store the output rather than a Butler collection. Eliminate use of Butler in the frontend, in favor of using that S3 bucket directly. This eliminated the ``CUTOUT_BUTLER_COLLECTION`` configuration setting and adds new ``CUTOUT_STORAGE_URL`` and ``CUTOUT_TMPDIR`` configuration settings.
- Use a different method of generating signed S3 result URLs that works correctly with workload identity in a GKE cluster. This adds a new ``CUTOUT_SERVICE_ACCOUNT`` configuration setting specifying the service account to use for URL signing. The workload identity the service runs as must have the ``roles/iam.serviceAccountTokenCreator`` role so that it can create signed URLs.

### New features

- Add new ``--reset`` flag to ``vo-cutouts init`` to wipe the existing database.

### Bug fixes

- Stop using a FastAPI subapp. This was causing problems for error handling, leading to exceptions thrown in the UWS handlers to turn into 500 errors with no logged exception and no error details.

## 0.1.0 (2021-11-11)

Initial version, which uses a colon-separated tuple as the `ID` parameter and has an initial proof-of-concept backend that runs `pipetask` via `subprocess` and does not do correct conversion of coordinates to pixels.

This is only a proof of concept release. Some of the things it does are very slow and block the whole asyncio process. The backend will be changed significantly before the first production release.
