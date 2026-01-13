import os, requests

APP_URL = os.environ["https://affiliate-hub-production.up.railway.app/"]  # fx din Railway URL
r = requests.get(APP_URL, timeout=60)
print("warmup status:", r.status_code)
r.raise_for_status()
