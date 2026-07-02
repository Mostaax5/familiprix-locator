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
threads = 8
timeout = 180
keepalive = 5
