# Security operations

No internet application can be guaranteed unhackable. This project uses layered
controls so that one failed control does not expose the store plan or write APIs.

## Employee accounts

New isolated deployments use individual owner, employee and viewer accounts.
See deploy/README.md for private bootstrap, server-side role checks, session
revocation and full backups. Public search remains intentionally public. Account
passwords are salted scrypt hashes; never log plaintext bootstrap secrets.

## Legacy single-store password

Generate a password hash locally:

```powershell
python generate_password_hash.py
```

Put only the generated hash in Render as the secret environment variable
`APP_PASSWORD_HASH`. Never put the plaintext password in Render, GitHub, source
files, screenshots, logs, or chat. Use a unique passphrase of at least 15
characters and store it in a password manager.

Changing `APP_PASSWORD_HASH` immediately invalidates every existing session.
The old public legacy digest is no longer accepted. Once employee accounts exist,
shared-password login is disabled. Rotating APP_PASSWORD_HASH only affects legacy
shared-password sessions; change/deactivate individual accounts to revoke theirs.

## Deployed protections

- Product search, product images, barcode lookup, and Client assistance are
  intentionally public so employees can answer customers immediately.
- Every product/location mutation, Scan action, Plan change, import, export,
  backup, and destructive action requires a server-validated session.
- Session tokens are random, stored only as SHA-256 fingerprints in the database,
  sent in `Secure`, `HttpOnly`, `SameSite=Strict` cookies, expire after 8 hours,
  and close after 30 minutes without activity.
- Writes require a separate CSRF token and a same-origin browser request.
- Login attempts and sensitive operations are rate limited and security events
  are audited without recording passwords, IP addresses, or raw user agents.
- Upload size, page, record, and parser-concurrency limits reduce denial-of-service
  risk. Stored external URLs are restricted to HTTPS.
- PostgreSQL connections require TLS. Outbound AI, catalogue, backup, and warmup
  requests require HTTPS and cannot redirect credentials to another host.
- Destructive database resets require an exact confirmation phrase and create a
  security audit event. CSV exports neutralize spreadsheet formulas.
- CSP, HSTS, clickjacking, MIME-sniffing, referrer, permissions, and cache headers
  reduce browser-side exposure.
- Gunicorn bounds request lines and headers and creates runtime files with a
  private process mask.
- Dependabot, dependency auditing, tests, and CodeQL run in GitHub Actions.

## Secret hygiene

- Keep `DEEPSEEK_API_KEY`, `GITHUB_TOKEN`, `DATABASE_URL`, and
  `APP_PASSWORD_HASH` only in Render secret environment variables.
- Full AI questions and answers are not persisted by default. Set
  `AI_LOGGING_ENABLED=1` only after deciding that the training-data retention is
  appropriate for the store and its customers.
- Give tokens the minimum permissions they need. Rotate them immediately after
  accidental disclosure or suspicious activity.
- Keep the backup Gist private and protect the GitHub account with MFA.
- Do not enable Flask debug mode in production.

## Incident response

If compromise is suspected, deactivate affected employee accounts (or rotate
`APP_PASSWORD_HASH` for legacy deployments), then rotate API,
GitHub, and database credentials. Review Render and GitHub logs, preserve evidence,
redeploy the current main branch, and verify `/healthz`. Password rotation closes
all active app sessions automatically.

## Isolation and remaining limits

Each store deployment uses a separate process, database and hostname. STORE_ID
binding checks prevent accidental database reassignment. The app does not route
multiple stores through a shared process. Public search/catalogue data remains
accessible without employee login. MFA/passkeys and self-service email recovery
are not implemented. Owners manage/reset employee access; recovery of the final
owner requires an operator-controlled database recovery procedure.
