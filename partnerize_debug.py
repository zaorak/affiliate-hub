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
    # print mere af body (Partnerize sender ofte en fejltekst der forklarer hvorfor)
    txt = (r.text or "").strip()
    print("body:", txt[:2000] if txt else "(empty)")
    return r

print("PARTNER_ID:", PARTNER_ID, "PUBLISHER_ID:", PUBLISHER_ID)

# 1) My brands (kræver ikke partnerId i path)
hit(f"{BASE}/v3/partner/my-brands")

# 2) Participations (kræver partnerId)
use_id = PARTNER_ID or PUBLISHER_ID

print("\n--- Testing participations param formats ---")

# Variant 1: repeated keys WITHOUT []
params_v1 = [
    ("page", "1"),
    ("page_size", "50"),
    ("status", "a"),
    ("status", "p"),
    ("status", "t"),
    ("status", "r"),
    ("campaign_status", "a"),
    ("campaign_status", "r"),
]
hit(f"{BASE}/v3/partner/{use_id}/participations", params=params_v1)

# Variant 2: comma-separated strings
params_v2 = {
    "page": "1",
    "page_size": "50",
    "status": "a,p,t,r",
    "campaign_status": "a,r",
}
hit(f"{BASE}/v3/partner/{use_id}/participations", params=params_v2)

# Variant 3: keep [] but as REPEATED keys (nogle gateways kræver det præcist)
params_v3 = [
    ("page", "1"),
    ("page_size", "50"),
    ("status[]", "a"),
    ("status[]", "p"),
    ("status[]", "t"),
    ("status[]", "r"),
    ("campaign_status[]", "a"),
    ("campaign_status[]", "r"),
]
hit(f"{BASE}/v3/partner/{use_id}/participations", params=params_v3)
