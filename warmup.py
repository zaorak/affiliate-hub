import os
import requests

APP_URL = os.environ["https://affiliate-hub-production.up.railway.app/"]  # fx https://affiliate-hub-production.up.railway.app/

r = requests.get(https://affiliate-hub-production.up.railway.app/, timeout=60)
print("warmup status:", r.status_code)
r.raise_for_status()
