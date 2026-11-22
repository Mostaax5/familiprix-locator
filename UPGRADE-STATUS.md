# Upgrade implementation status

Baseline: `5f8daef379234c25a8c1260b7dc24feac74aa95a`.
Branch: `upgrade/reliability-architecture`.

## Implemented

- Atomic stale-write protection for full product updates and layout autosaves,
  including version propagation through the compact catalogue API.
- Barcode response ordering protection and bounded API request-header waits
  with caller cancellation. Body-stream timeouts remain a follow-up.
- Version 2 browser import, consistent database export snapshots and restored
  aisle ordering; a real JSON export/import regression test.
- Locally pinned scanner and OCR dependencies, licenses and checksums.
- Durable, session-owned PDF jobs with leased claims, heartbeat, restart retries,
  stale-worker fencing, retention and embedded/separate-worker execution.
- Explicit migration command and tooling option to suppress import-time services.
- Process liveness and cached database dependency diagnostics; UI preserves the
  database-unavailable response rather than silently defaulting to SQLite.
- Independent layout domain rules and less synchronous snapshot work on edits.
- Regression tests for conflicts, request ordering, cancellation, backup restore,
  jobs and dependency reporting; PostgreSQL CI integration workflow.

## Accuracy and free-operation upgrade

- Exact entity/barcode validation; no cross-product hero images or inherited UPCs.
- Decimal and bilingual package matching, unit conversion, concentration, count,
  dosage form and known product-family conflict checks.
- Trusted exact source selection before completeness/image scoring.
- Verified-only public images/descriptions and customer-assistant evidence;
  raw legacy values preserved. Repair of falsely labelled cached image evidence.
- Immediate cached UPC lookup with separate missing-image enrichment.
- Local catalogue assistance by default, paid model/embedding calls blocked;
  optional bounded Cloudflare Workers Free inference with local fallback.
- No generated usage text copied into product descriptions.
- Offline reversible JSON catalogue cleanup preserving UPCs and placements.
- 26 synthetic matching regressions: all pass, versus 7 on the preceding commit.
  This is not measured real-store accuracy.

See FREE-OPERATION.md for configuration and limits. No production data backup,
new hosting deployment, per-employee authentication or multi-store tenancy has
been completed. The requested full rebuild remains unfinished.

## Remaining work from the architecture review

The full roadmap is **not complete**. This release is its correctness and
execution foundation. Remaining work includes:

- Production database connectivity diagnosis, database backup and restore drill.
- Physical-device scanner/OCR testing under the deployed security headers.
- Moving enrichment, embeddings, image fetching, quality audits and AI jobs to
  the durable worker, with idempotency/checkpoints specific to each operation.
- Replacing the remaining import-time lifecycle with an application factory.
- Durable catalogue revisions and incremental cache/index updates across workers.
- Separating catalogue identity from placement through a tested data migration.
- Frontend ES modules, unified state management, asynchronous snapshot storage
  and per-feature loading; module extraction of the large search/AI components.
- Representative AI evaluation fixtures, measured provider round-trip reduction,
  real PostgreSQL query plans and mobile/concurrent load testing.
- Production rollout after the data backup, PostgreSQL checks and device tests.

No production deployment or database modification was performed by this work.
The original source backup remains separate in `/workspace/backups`.

## Rollout and rollback

1. Export store data and take a database-provider backup; verify both in isolation.
2. Run tests and migrations in staging with the actual Python/PostgreSQL versions.
3. Validate product/layout conflict UX, PDF restart recovery, iPhone/Android scan,
   JSON backup round-trip and database-outage behavior.
4. Choose embedded or external job execution; start exactly the intended workers.
5. Deploy the reviewed commit and monitor dependency availability, job completion,
   conflicts, latency and memory. Keep the baseline deployment available.

The added jobs table is additive. Roll back application code without dropping it.
Allow active jobs to finish or re-upload affected PDFs when returning to the
legacy file-based parser. Do not restore an old database snapshot over valid
new store edits merely to roll back application code.
