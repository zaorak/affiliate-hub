import os
import base64
import requests

BASE = (os.getenv("PARTNERIZE_BASE") or "https://api.partnerize.com").rstrip("/")
APP_KEY = (os.getenv("PARTNERIZE_APP_KEY") or "").strip()
API_KEY = (os.getenv("PARTNERIZE_USER_API_KEY") or os.getenv("PARTNERIZE_API_KEY") or "").strip()
PARTNER_ID = (os.getenv("PARTNERIZE_PARTNER_ID") or "").strip()
PUBLISHER_ID = (os.getenv("PARTNERIZE_PUBLISHER_ID") or "").strip()

token = base64.b64encode(f"{APP_KEY}:{API_KEY}".encode("utf-8")).decode("ascii")
headers = {"Accept": "application/json", "Authorization": f"Basic {token}"}

def hit(url, params=None):
    r = requests.get(url, headers=headers, params=params or {}, timeout=60)
    print("\nGET", r.url)
    print("status:", r.status_code)
    print((r.text or "")[:500])
    return r

print("PARTNER_ID:", PARTNER_ID, "PUBLISHER_ID:", PUBLISHER_ID)

# 1) My brands (kræver ikke partnerId i path)
hit(f"{BASE}/v3/partner/my-brands")

# 2) Participations (kræver partnerId)
use_id = PARTNER_ID or PUBLISHER_ID

params = [
    ("page", 1),
    ("page_size", 50),

    # vigtig ændring: brug "status" og "campaign_status" uden []
    ("status", "a"),
    ("status", "p"),
    ("status", "t"),
    ("status", "r"),

    ("campaign_status", "a"),
    ("campaign_status", "r"),
]

hit(f"{BASE}/v3/partner/{use_id}/participations", params=params)
