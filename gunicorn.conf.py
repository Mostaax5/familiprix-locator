# Gunicorn auto-loads this file from the working directory, so these settings
# apply even if the Render service's start command is just "gunicorn app:app".
#
# threads: without them ONE request occupies the whole (single-worker) server —
# a 10-12s AI call froze every phone in the store. 8 threads let the fast
# endpoints keep answering while an AI/lookup request waits on the network.
# timeout: parsing a multi-MB planogram PDF takes well over gunicorn's default
# 30s on Render's small CPU — the default killed the worker mid-parse and the
# upload failed with "Erreur réseau". Normal requests finish in milliseconds.
workers = 1          # Render free tier: one small CPU — more workers just swap
# 4 threads (was 8): still lets a slow AI call run without blocking other phones,
# but halves how many requests can pile up their working memory at once on the
# 512 MB instance. PDF parsing is additionally serialized (see import_export.py).
threads = 4
timeout = 180
keepalive = 5
# Recycle the worker every ~300 requests (± jitter): Python/pdfplumber don't always
# return freed memory to the OS, so over a long uptime RSS creeps up until the
# 512 MB cap kills the instance. A periodic clean restart keeps memory flat, and
# with only one worker gunicorn drains in-flight requests before replacing it.
max_requests = 300
max_requests_jitter = 40
