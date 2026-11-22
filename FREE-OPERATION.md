# Free operation and automatic product matching

## Employee workflow

A known UPC resolves from the local catalogue without waiting for online lookup.
Missing pictures are fetched separately. Exact package identity and trusted source
evidence allow automatic attachment; contradictory, ambiguous or unsupported
metadata is withheld. Employees can still locate the product using its UPC,
planogram name and location. No metadata review is required to keep using search.
Manual corrections remain available as an optional administrative tool.

Descriptions and pictures are facts about a specific package, not generated
content. Matching compares UPC/GTIN, strength, concentration, size, count, dosage
form and known variant/family differences. Structured product pages must put the
barcode and metadata on the same entity. A related product elsewhere on a page
cannot validate its headline or hero image. Trusted exact results outrank richer
open-database results. Unknown descriptions do not enter public cards, search
text or customer-assistant evidence. Original values remain in the database.

The name/family vocabulary is finite. Exact UPC source data can itself be wrong;
these rules reduce errors but cannot prove real-world accuracy. The 26-case
synthetic benchmark (`python evaluate_matching.py`) is regression coverage,
not a store acceptance study. Measure wrong attachments, metadata coverage and
search success against a representative real catalogue before rollout.

## No paid AI by default

`AI_ALLOW_PAID=0` is the default, including when old OpenAI/Gemini/Kimi keys are
present. Paid embeddings are disabled as well. The customer assistant works
locally from catalogue evidence without an API key. The UI identifies this as a
catalogue answer. Generated usage notes are never copied into descriptions.

Optional Cloudflare Workers AI is supported with these private settings:

- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_AI_TOKEN`
- `CLOUDFLARE_WORKERS_FREE_CONFIRMED=1`, only on an actual Workers Free account

The fixed model is `@cf/meta/llama-3.1-8b-instruct-fp8-fast`. Calls have bounded
payloads, output and timeouts. Quota/error responses fall back to local answers;
there is no retry or paid-model fallback. No Cloudflare account has been connected
or live inference tested by this change. The confirmation setting is deployment
configuration, not an automatic billing-plan check. Do not set it on a paid
account. Official pricing: https://developers.cloudflare.com/workers-ai/platform/pricing/

Keep `AI_ALLOW_PAID=0`, `AI_AUTO_ENRICH=0`, `AI_DEEP_LOOKUP=0` and
`SEMANTIC_SEARCH_ENABLED=0` for this project's $0 constraint. Legacy paid-provider
code remains for compatibility but is blocked unless explicitly enabled.

## Reversible catalogue cleanup

Before any production change, obtain the original JSON export and a full database
backup. The source-code backup does not contain production store data.

```
python rebuild_catalogue.py store-export.json /private/new-cleanup-directory
```

This offline tool creates an unchanged `original.json`, `clean-catalogue.json`,
`quarantine.json` and SHA-256 manifest. It preserves string UPCs including leading
zeros, product IDs, names, placements, layout and expiry dates. It withholds old
automatic/unproven descriptive fields and preserves actual manual field evidence.
It refuses numeric UPCs or an existing output directory. It never accesses the
production database. Test the cleaned export in an isolated database first.

`python migrate.py` also repairs the old image worker's misleading
“Manual verified exact GTIN cache” provenance. Real employee-confirmed evidence
is retained. Raw image URLs are retained; the repair is idempotent.

## Hosting decision and remaining release work

Dawnlight's repository documents a Vercel frontend/auth proxy with Neon Postgres,
Auth and Functions. Its connected Neon organization reports the free plan.
That is useful architectural evidence, not proof that every service, commercial
use or workload is free. Do not provision paid plans or assume Vercel Hobby
commercial eligibility. No hosting migration or new service was deployed here.

The current Render blueprint remains on its free plan. The Flask app still has
PDF/OCR and long-running background work, so copying it unchanged into request-
scoped serverless functions would break lifecycle assumptions. A migration must
separate the static frontend, authenticated API, persistent data and durable jobs,
and test all existing features. Free tiers also have quotas/cold starts; $0 does
not establish an always-on service guarantee.

Individual accounts, per-store process/database isolation, deployment packaging
and a full local backup/restore check are now implemented; see deploy/README.md.
Commercial readiness still needs representative source/accuracy checks,
physical-device and concurrent workflow testing, and an actual hosting cutover.

The live database remains unreachable. A read-only cache recovery preserved
5,521 placements and 5,448 distinct UPCs. This is a partial product recovery,
not a full production backup: expiry history, field evidence, original layout
configuration and other database records remain missing. Production was not
modified. The recovery copy and its original bytes are stored privately.
