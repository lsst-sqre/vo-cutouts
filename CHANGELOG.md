# Change log

vo-cutouts is versioned with [semver](https://semver.org/).
Dependencies are updated to the latest available version during each release, and aren't noted here.

Find changes for the upcoming release in the project's [changelog.d directory](https://github.com/lsst-sqre/vo-cutouts/tree/main/changelog.d/).

<!-- scriv-insert-here -->

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
