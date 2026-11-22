# Isolated store deployment

Each store gets its own hostname, app process and database. One process never
switches databases based on a request header, URL, browser storage or AI payload.
Caches and background jobs therefore stay inside that store's process/database.
This is separate-instance isolation, not shared-process multi-tenant SaaS.

`STORE_ID` is bound into `app_settings` during initialization. Every subsequent
connection checks that binding. A different store ID is rejected; it cannot
silently rename or claim an existing store's database. For the current Richelieu
store, retain `STORE_ID=richelieu`, including its expiry records. Set a distinct
ID, database, hostname and bootstrap secret for each new store. Separate local
Compose deployments also need different project names and host ports.

## Run on an existing computer

This avoids a hosting subscription and cloud cold starts. The store's computer,
network, power and backup availability determine uptime. Use the supplied Docker
image with a persistent volume; never put the SQLite file inside an ephemeral
cloud container. This command is a localhost setup, not a public HTTPS server.

1. Set `STORE_ID`, `STORE_NAME`, optionally `STORE_ADDRESS`, and
   `OWNER_PASSWORD_FILE` in a private environment file. The password file must be
   readable by container UID 10001. Use a unique passphrase of at least 15 characters.
2. Run `docker compose up --build -d`.
3. Open `http://localhost:10000`, sign in as `owner`, and create employee accounts.
4. For phones/remote access, put the app behind your existing HTTPS reverse proxy,
   set `APP_EXTERNAL_URL=https://your-store-host`, `AUTH_COOKIE_SECURE=1`, and
   `TRUST_PROXY=1` only when there is exactly one trusted proxy. Expose only the
   proxy; the Compose port is bound to localhost. Mobile camera access needs HTTPS.

`deploy/start.py` refuses missing store configuration, public HTTP or insecure
cookies, paid AI, a mismatched database, or a new database without an initial
owner secret. Redeployment never resets existing accounts from an old bootstrap
secret. It removes the plaintext bootstrap environment variable before starting
Gunicorn. Remove the original bootstrap secret after first setup.

For an existing deployment, run `python migrate.py`, then bootstrap exactly one
owner with `python manage_employees.py owner --name 'Owner name'`. The CLI prompts
privately; automation can pipe a secret using `--password-stdin`. There is no
public signup or first-visitor-becomes-owner endpoint. Once any account exists,
shared-password login is disabled, including when all accounts are inactive.

## Cloud option with no paid plan

`deploy/render-free.yaml` describes a new Render Free web service with HTTPS and
a separately configured database. It intentionally creates no Render PostgreSQL
resource. Use a free persistent database, such as Neon Free, with its TLS
connection string in the service's secret `DATABASE_URL`. Confirm free-plan
quota and commercial-use terms on the actual accounts before provisioning.

Dawnlight confirms that this owner's existing projects use Neon, but its Vercel
frontend is not a drop-in host for this Flask/PDF worker application. A new
serverless port is unnecessary for the isolated deployment and could break PDF
uploads and background tasks. No paid plans, domains or AI providers are needed
by the code. Cloud free-tier quotas and cold starts still apply; do not promise
unlimited stores or always-on service for free. Separate stores may need their
own free accounts/workspaces rather than sharing one free-hour allowance.

This change has not provisioned or migrated a live hosting service. The existing
Render service's database remains unavailable; deploying over it would not
recover missing expiry/account/history records. Keep production intact until
recovery and staging checks are complete.

## Accounts and normal work

Owners manage accounts, backups/imports and bulk catalogue operations. Employees
can edit store products and locations. Viewers cannot write store data. All
account writes use the existing server session, CSRF and origin checks. A role,
password, login or activation change revokes the affected account's sessions.
Last-owner checks are serialized in the database, including across processes.
Per-account failed-login limits persist across restarts. Public product lookup
remains public; account roles do not make the public catalogue private.

Employees do not approve metadata matches. Reliable matching applies details
automatically; unknown descriptions and pictures remain withheld. Manual admin
corrections are optional. Account login/password changes are separate from
product verification.

## Backup and recovery

For SQLite, `python backup_store.py /private/new-backup.sqlite` uses SQLite's
consistent backup API, checks database integrity, refuses overwriting any file,
and creates an owner-only file. It includes accounts, settings and UPCs. Copy
backups to a separate location; a file on the same failed disk is not disaster
recovery. Schedule this command using the host's scheduler if daily backups are
required. No schedule has been installed automatically.

For PostgreSQL use a provider snapshot or `pg_dump` with a direct, unpooled
connection and a compatible client version. The app's JSON export contains
business data, not employee credentials or a complete cluster backup.

Restore full SQLite backups to a new private path and verify them before changing
`DB_PATH`. Keep the same `STORE_ID`. On a restored copy, revoke old sessions with
`UPDATE auth_sessions SET revoked_at=1` before exposing it to users. Restore all
PostgreSQL data in isolation before changing any production connection.

`recover_cached_catalogue.py` builds a partial recovery from a saved public
`/api/products` array. It retains original bytes, original row IDs, string UPCs
and locations, quarantines uncertain descriptions/photos, and reconstructs only
observed layout bounds. It cannot recover empty fixtures, labels, expiry history,
accounts, unplaced reference products or original field evidence. Its manifest
explicitly marks those omissions. It never accesses the production database.
