import os


# Gunicorn auto-loads this file from the working directory, so these settings
# apply even if the Render service's start command is just "gunicorn app:app".
#
# threads: without them ONE request occupies the whole (single-worker) server —
# a 10-12s AI call froze every phone in the store. 8 threads let the fast
# endpoints keep answering while an AI/lookup request waits on the network.
# timeout: parsing a multi-MB planogram PDF takes well over gunicorn's default
# 30s on Render's small CPU — the default killed the worker mid-parse and the
# upload failed with "Erreur réseau". Normal requests finish in milliseconds.
# Bind explicitly so Render can detect the web service immediately. Render sets
# PORT for every web service; 10000 is its documented local/default value.
bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"

workers = 1          # Render free tier: one small CPU — more workers just swap
# Two threads keep fast requests available while one AI call waits on the
# network, without allowing four catalogue searches to stack their temporary
# working sets inside Render's 512 MB process.
threads = 2
timeout = 180
keepalive = 5
# Bound parser work before a request reaches Flask. These are comfortably above
# every legitimate app request while limiting oversized request-line/header abuse.
limit_request_line = 4094
limit_request_fields = 50
limit_request_field_size = 8190
# Any runtime files created by the worker are private to its operating-system user.
umask = 0o077
# Recycle the worker every ~500 requests (± jitter): Python/pdfplumber don't
# always return freed memory to the OS, so over a long uptime RSS creeps up.
# Search caches warm themselves after a restart, while this bound prevents
# allocator fragmentation from accumulating until Render kills the process.
# (A recycle can kill a background plano parse — the status endpoint detects the
# dead pid and relaunches it from the stored PDF, so jobs self-heal.)
max_requests = 500
max_requests_jitter = 50
