# Upgrade implementation status

Baseline: `5f8daef379234c25a8c1260b7dc24feac74aa95a`.
Branch: `upgrade/reliability-architecture`.

## Implemented

### Catalogue redesign

- New normalized package identities, preserved placement mapping, source history
  and transactional cross-worker revisions with incremental projection/indexing.
- New mobile/desktop finder, bilingual fact search, category/aisle filters,
  all-location details, comparison and a lazy camera decoder.
- New employee home screen; established inventory/write tools remain compatible.
- Description-independent retrieval, contextual label expansion and automatic,
  resumable per-package source acquisition. Supplier feeds can grow references.
- Optional schema-constrained local Ollama/Workers Free query interpretation;
  no generated facts or descriptions, no paid inference or silent substitutions.
- All 5,448 recovered UPC searches pass, all 5,521 placements match; source
  snapshots remain private. Three bounded live Familiprix source checks succeed.

See CATALOGUE-ARCHITECTURE.md for current behavior and measured limits. The new
catalogue replaces the older finder as the primary UI. The remaining-work list
below records broader legacy/production work, not a completed commercial rollout.

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

## Employee accounts, isolated deployment and cache recovery

- Individual owner/employee/viewer accounts and mobile account-management UI.
- No public registration or legacy password bypass; shared login stops once
  employee accounts exist. Session revocation, owner-only imports/backups,
  persistent per-account login throttling and a serialized last-owner guard.
- Per-store process/database isolation, database binding checks and server-owned
  store labels. Expiry endpoints ignore forged store selectors in isolated mode.
- Non-root Docker/Compose deployment and a separate Render Free blueprint;
  validated HTTPS/secure-cookie/zero-paid-AI startup and private owner bootstrap.
- Complete SQLite backup with integrity and restoration checks, including accounts.
- Partial live-cache recovery: 5,521 placements, 5,448 distinct UPCs and six
  reconstructed aisle configurations. Original row IDs, UPCs and locations match
  exactly. Original bytes and 25,324 quarantined fields are retained privately.
- Isolated recovered-catalogue check: 25/25 exact UPC queries returned the requested
  item; warm median 24.3 ms, maximum 26.9 ms locally. This is not internet latency,
  concurrent load testing or description/photo accuracy ground truth.
- Real mobile browser workflow: owner login, account creation, employee permissions,
  password change, session restoration and persisted database records passed.

See deploy/README.md and FREE-OPERATION.md. No live deployment or full production
backup exists yet. Missing production data includes expiry history, original
layout details, accounts/history and unplaced reference products. Shared-process
multi-tenancy is intentionally not the isolation model. Commercial acceptance,
actual hosting cutover and physical-device testing remain incomplete.

## Remaining work from the architecture review

The full roadmap is **not complete**. This release is its correctness and
execution foundation. Remaining work includes:

- Production database connectivity diagnosis, database backup and restore drill.
- Physical-device scanner/OCR testing under the deployed security headers.
- Moving enrichment, embeddings, image fetching, quality audits and AI jobs to
  the durable worker, with idempotency/checkpoints specific to each operation.
- Replacing the remaining import-time lifecycle with an application factory.
- Migrating legacy writes completely off the compatibility placement table;
  the new catalogue already separates package identity and placement reads.
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
