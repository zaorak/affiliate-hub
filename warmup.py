import os
import requests

APP_URL = os.environ["APP_URL"].rstrip("/")
url = APP_URL + "/?warmup=1"

r = requests.get(url, timeout=180)
print("warmup status:", r.status_code)
r.raise_for_status()
