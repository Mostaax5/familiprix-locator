# Familiprix Locator

French-language product search, barcode placement, store layout, expiry tracking
and evidence-grounded customer assistance. Python 3.13.7, Flask and PostgreSQL;
SQLite is supported for isolated development.

## Run locally

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python migrate.py
python app.py
```

Without `DATABASE_URL`, the application creates `familiprix.db` locally. Never
use production credentials for development. Generate a password hash with
`python generate_password_hash.py` and configure `APP_PASSWORD_HASH` privately.
See SECURITY.md and SCANNER.md before changing authentication or decoding.

## Background PDF jobs

Accepted planogram uploads are stored in `background_jobs`, together with the
session owner, result and lease state. The small deployment defaults to one
embedded consumer. For a separate worker, set `JOB_EXECUTION_MODE=external` on
the web service and run `python worker.py` with the same database configuration.
Run `python migrate.py` before switching traffic. No second service is provisioned
automatically. Use managed PostgreSQL for restart/redeploy durability; a SQLite
file on an ephemeral host does not provide that guarantee.

A claim leases a job for 300 seconds; the worker renews every 30 seconds.
A crashed job can be claimed up to three times. Completion is fenced by a random
lease token. Invalid PDFs are terminal failures. Source bytes are removed on
completion; terminal job records expire after 24 hours. PDFs are capped by the
existing upload limits and stored as base64 text for portable database support.
At higher upload volumes, move payloads to durable object storage and retain
only an object reference in the job row. Other enrichment jobs still use their
existing execution paths; this release migrates PDF parsing only.

`APP_START_SERVICES=0` suppresses app import-time initialization and background
services for tooling. Call `python migrate.py` explicitly when using that mode.

## Availability

- `/livez`: request process is running.
- `/readyz`: a catalogue snapshot is ready for cached search (existing contract).
- `/health/dependencies`: recent database connectivity, separately from cached
  search. Probe results are cached for 10 seconds; 503 means unavailable/unknown.
  It does not certify database write privileges or every application dependency.
- `/api/system/info`: application configuration and detailed catalogue status.

## Write and backup contracts

Full product and layout edits require `expected_modified_at`. Clients retain the
server's opaque timestamp and submit it with the edit. The update compares that
value atomically. Missing preconditions return 428; stale edits return 409.
Reload and reconcile the user's intended change; never blindly retry an old
full object against a new version. New writes use microsecond timestamps.

JSON backup versions 1 and 2 are accepted. Version 2 exports run under a
consistent read transaction and preserve aisle order on import. A JSON export
covers business tables, not the entire PostgreSQL cluster. A provider snapshot
or pg_dump is required for full recovery, including jobs and other operational
records. Verify restoration in an isolated database before a production migration.

## Scanner assets

Quagga 1.10.2, Tesseract 5.1.1/core 5.1.1, English trained data 1.0.0 and ZXing
0.21.3 are served locally. OCR worker, core and language paths are explicitly
local. Licenses and SHA-256 hashes are in `static/vendor/`. OCR assets load only
when needed. CSP permits WebAssembly compilation without enabling arbitrary
JavaScript eval. Test physical iPhone/Android devices before production rollout.

## Verification

```sh
pip install pytest==8.4.2
python -m pytest -q tests
for test_file in tests/*.js; do node "$test_file"; done
```

Optional PostgreSQL integration tests use **only** `TEST_POSTGRES_URL`, never
`DATABASE_URL`. They create and remove an isolated random schema in a disposable
test database. CI runs them using a PostgreSQL service.

See UPGRADE-STATUS.md for implemented scope and remaining architecture work.
