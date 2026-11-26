# Catalogue redesign

The finder identifies and retrieves packages without relying on descriptions.
`/` is a new mobile-first finder. `/operations` is the employee workspace, with
existing placement, planogram, expiry and account write flows retained during
migration. This is a new catalogue/search layer and primary interface, not a
claim that every historical module has been rewritten.

## Product identity, facts and location

`catalogue_items` has one record per exact package. Valid equivalent GTIN
representations share an identity; invalid store codes remain exact-only and
leading zeroes are retained. Barcode-less products remain separate records.
`catalogue_placements` maps every original placement ID to that identity and its
original barcode. Multiple shelf locations appear on one product card.

Existing `products` remains the placement write model. The rebuildable projection
bridges existing tools and the new catalogue without destructively rewriting
source data. Reference-only products remain searchable without an invented
location or stock. Authorized supplier/manufacturer feeds can add more reference
products: `python catalogue_import.py feed.json --source manufacturer`.
Feeds require string barcodes and names; all records are validated before writes.

Cards use deterministic expansion of store-label abbreviations, package size,
strength, count, form, flavour and source-backed fields. Original labels remain
available in details. Parsed labels are not manufacturer verification. Context
differentiates `FIL/DENT`, `BR/DENT`, denture cleaners and toothpaste. Unknown
abbreviations stay unchanged. Vitamins in skin cream do not make it an oral
vitamin. Conflicting duplicate label attributes remain unknown.

Descriptions are optional source text in details, never generated or indexed for
retrieval. Photos require an established package link. No product is removed
because an image, description or other field is missing.

## Retrieval and AI

The inverted index combines weighted lexical ranking, bilingual vocabulary,
product-type concepts, explicit package constraints and exact identifiers. A
250 ml package can match a 0.25 l query. Unknown UPC digits are never silently
corrected. Typo suggestions are labelled. Sugar-free and similar claims require
explicit evidence. Pagination and filters return bounded results rather than
sending the entire catalogue to a phone. Details show all known shelf positions;
comparison highlights differing facts, without claiming therapeutic equivalence.

AI interprets previously unmatched employee requests into known product-type
categories. It cannot create facts, alter UPCs, attach images, select therapeutic
substitutes or write catalogue records. Schema-invalid answers are rejected;
known brand/name anchors and explicit constraints remain. Category suggestions
are labelled as interpretations. Medical suitability remains a pharmacist task.

The default requires no model. Optional adapters support an already-running
Ollama model on loopback (`CATALOGUE_OLLAMA_URL=http://127.0.0.1:11434` and
`CATALOGUE_OLLAMA_MODEL=<installed model>`) or the existing explicitly configured
Cloudflare Workers Free adapter. No model download, subscription or key is
created. Calls require an employee session, are single-flight per process,
bounded to four seconds and cached briefly. Failure leaves ordinary search
available. Live model inference has not been validated in this workspace.

## Source acquisition

A durable task exists per package, not per shelf. It tries Familiprix's exact
entity parser, using the store code when available, then a bounded Open Food
Facts fallback. Source/identity checks decide automatic attachment. Unresolved
observations stay stored; employees do not approve each match. Authorized feeds
provide another coverage path without depending on public-web availability.

`catalogue_sources` stores current observations; `catalogue_observations` retains
distinct historical payloads. Jobs have a 180-second lease, fenced publication,
retry backoff and a shared database throttle of one claim per 30 seconds.
Accepted sources refresh after 30 days; missing records retry after a day. At
5,448 products, an initial pass needs roughly two days of continuous execution,
longer with sleeping hosts/timeouts. Source access cannot guarantee coverage.

An embedded worker starts after database startup. For a separate existing machine,
set `CATALOGUE_SOURCE_WORKER=0` on web and run `python catalogue_worker.py` with
the same store/database configuration. No extra service is provisioned.
`APP_START_SERVICES=0` disables startup workers for tooling. Old description,
image and embedding maintenance is disabled by default;
`LEGACY_CATALOGUE_MAINTENANCE=1` is a compatibility switch. PDF jobs remain separate.

## Consistency and deployment

Database triggers maintain a transactional revision and changed-record queue for
placements, references, evidence and source updates. Imports, deletions, barcode
changes, direct SQL and rolled-back writes are covered. PostgreSQL locks the
revision row before source-table writes, in the same order as projection
publication. Only affected packages/posting lists are rebuilt; in-flight
requests keep immutable snapshots. Placement counts are checked before publication.

`/readyz` waits for the new index and database dependency check. Initial building
is separate from health requests. Search performs no remote source/model calls.
Frontend modules revalidate across deployments. Tables are additive; rollback
does not require restoring an older database over new store edits. Full SQLite
backups/PostgreSQL snapshots include source history and jobs; the older business
JSON export is not a full backup of these operational tables.

## Verification and limits

Run Python tests, `tests/e2e_catalogue.cjs` and `tests/e2e_employee_accounts.cjs`.
Browser scripts require Playwright and Chromium, installed by CI.
`python evaluate_catalogue.py /private/recovered.sqlite` opens the input read-only
and makes a temporary copy for mutation tests. It checks every UPC, exact
placement preservation, payload size, query speed and incremental updates.

The recovered copy retained 5,448 UPCs and 5,521 placements. All exact UPC queries
passed. Package attributes were extracted for 3,697 products from existing
labels. Three bounded live checks found exact Familiprix records with photos and
factual fields. This is not independent validation of every fact, a full source
enrichment run, clinical validation or commercial certification.

Production is unchanged. Render access and the original database are still
needed for complete migration/recovery. Cache recovery lacks accounts, expiry
history and original layout configuration. Physical camera testing, a labelled
accuracy evaluation, hosting validation and a store pilot remain release work.
No service has been purchased.
