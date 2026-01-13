import os
import requests

APP_URL = os.environ["APP_URL"]

r = requests.get(APP_URL, timeout=60)
print("warmup status:", r.status_code)
r.raise_for_status()
