# app.py
import os, json, sqlite3, threading, smtplib, datetime as dt, requests, io, csv, re
from urllib.parse import urlencode, quote
from email.message import EmailMessage
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
import streamlit as st
import time
import urllib.parse

# ✅ MUST be first Streamlit call
st.set_page_config(
    page_title="Publisher Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------- Setup & config --------------------
load_dotenv()

# --- Streamlit Cloud secrets → env shim (safe to keep locally too)
try:
    for k, v in st.secrets.items():
        os.environ.setdefault(k, str(v))
except Exception:
    pass

st.title("Publisher Dashboard")

# -------------------- Warmup (preload caches) --------------------
def _get_query_param(name: str) -> str:
    # Kompatibel med både nye og gamle Streamlit versioner
    try:
        v = st.query_params.get(name, "")
        if isinstance(v, list):
            return str(v[0]) if v else ""
        return str(v)
    except Exception:
        try:
            qp = st.experimental_get_query_params()
            v = qp.get(name, [""])
            return str(v[0]) if v else ""
        except Exception:
            return ""

is_warmup = _get_query_param("warmup") == "1"

if is_warmup:
    # Brug samme countries som du kører normalt
    env_countries = os.getenv("AWIN_COUNTRY", COUNTRY)
    warm_countries = [c.strip().upper() for c in env_countries.split(",") if c.strip()]

    # AWIN programmes (per country)
    for cc in warm_countries:
        try:
            cached_awin_programmes(cc)
        except Exception:
            pass

    # AWIN feeds list (hvis du har cached wrapper)
    try:
        # hvis du bruger cached_awin_feed_map()
        cached_awin_feed_map()
    except Exception:
        pass

    # Addrevenue advertisers/relations (per country)
    for cc in warm_countries:
        try:
            cached_addrev_list_advertisers(cc)
        except Exception:
            pass

    # Impact (global)
    try:
        cached_impact_programs()
    except Exception:
        pass

    # Impact catalogs/feeds
    try:
        # hvis du har wrapper:
        cached_impact_catalog_feeds_by_campaign()
    except Exception:
        try:
            # hvis du bruger direkte funktionen:
            impact_catalog_feeds_by_campaign()
        except Exception:
            pass

    # Partnerize (global)
    try:
        cached_partnerize_participations()
    except Exception:
        pass

    try:
        cached_partnerize_feeds_by_campaign()
    except Exception:
        pass

    st.write("Warmup complete")
    st.stop()
# -------------------- End warmup --------------------

API_BASE = "https://api.awin.com"
TOKEN   = os.getenv("AWIN_TOKEN")
PUB_ID  = os.getenv("AWIN_PUBLISHER_ID")
COUNTRY = os.getenv("AWIN_COUNTRY", "")
REGION  = os.getenv("AWIN_REGION", "")
DB = os.getenv("DB_PATH", "state.sqlite3")

# ---- AWIN product feed settings ----
AWIN_FEED_APIKEY  = os.getenv("AWIN_FEED_APIKEY", "").strip()
AWIN_FEED_LANG    = (os.getenv("AWIN_FEED_LANG") or "en").strip()
AWIN_FEED_FORMAT  = (os.getenv("AWIN_FEED_FORMAT") or "xml").strip()

# ---- SMTP / Alerts
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
ALERT_TO   = os.getenv("ALERT_TO")
ALERT_FROM = os.getenv("ALERT_FROM")

# Optional alert toggles
ALERTS_ENABLED        = os.getenv("ALERTS_ENABLED", "true").lower() == "true"
ALERT_ON_NEW          = os.getenv("ALERT_ON_NEW", "true").lower() == "true"
ALERT_ON_REMOVED      = os.getenv("ALERT_ON_REMOVED", "true").lower() == "true"
ALERT_ON_CLOSED       = os.getenv("ALERT_ON_CLOSED", "true").lower() == "true"
ALERT_ON_FEED_FAILURE = os.getenv("ALERT_ON_FEED_FAILURE", "true").lower() == "true"
ALERT_COOLDOWN_MIN    = int(os.getenv("ALERT_COOLDOWN_MIN", "60"))
FEED_ALERT_STATE = {}  # throttle feed-failure emails per country

DB_LOCK = threading.Lock()

# -------------------- DB --------------------
def db():
    con = sqlite3.connect(DB, timeout=30, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=5000;")
    con.execute(
        """CREATE TABLE IF NOT EXISTS programmes (
            advertiser_id INTEGER,
            name TEXT,
            status TEXT,
            relationship TEXT,
            country TEXT,
            first_seen TEXT,
            last_seen TEXT,
            PRIMARY KEY (advertiser_id, country)
        )"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS alert_log (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             ts TEXT,
             event TEXT,
             country TEXT,
             advertiser_id INTEGER,
             name TEXT,
             details TEXT,
             email_sent INTEGER,
             email_info TEXT
         )"""
    )
    return con

# -------------------- API helpers (AWIN) --------------------
def get_programmes(country_code: str):
    params = {"accessToken": TOKEN, "countryCode": country_code}
    url = f"{API_BASE}/publishers/{PUB_ID}/programmes?{urlencode(params)}"
    r = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=30)
    r.raise_for_status()
    return r.json()

@st.cache_data(show_spinner=False, ttl=12*60*60)  # 12 timer
def cached_awin_programmes(country_code: str):
    return get_programmes(country_code)

def get_fx_rate(base: str, target: str) -> float:
    if not base or not target or base.upper() == target.upper():
        return 1.0
    try:
        r = requests.get(
            "https://api.exchangerate.host/convert",
            params={"from": base.upper(), "to": target.upper(), "amount": 1},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json() or {}
        return float(data.get("result") or 1.0)
    except Exception:
        return 1.0

def awin_cread_link(advertiser_id: int, clickref: str | None = None, dest_url: str | None = None) -> str:
    """
    Build a proper redirect link:
      https://www.awin1.com/cread.php?awinmid=...&awinaffid=...&clickref=...&ued=<url-encoded-destination>
    If dest_url is None, we omit 'ued' (valid; matches your example).
    """
    try:
        mid = int(advertiser_id)
    except Exception:
        mid = advertiser_id
    params = {
        "awinmid": mid,
        "awinaffid": int(PUB_ID),
    }
    if (clickref or "").strip():
        params["clickref"] = clickref.strip()
    if (dest_url or "").strip():
        params["ued"] = dest_url.strip()
    return "https://www.awin1.com/cread.php?" + urlencode(params)

# -------------------- Addrevenue (optional second network) --------------------
ADDREV_BASE = os.getenv("ADDREV_BASE", "https://addrevenue.io/api/v2").rstrip("/")
ADDREV_TOKEN = os.getenv("ADDREV_TOKEN")
ADDREV_DEFAULT_CCY = (os.getenv("ADDREV_DEFAULT_CURRENCY") or "EUR").upper()
ADDREV_CHANNEL_ID = os.getenv("ADDREV_CHANNEL_ID")  # optional

def _addrev_headers():
    if not ADDREV_TOKEN:
        raise RuntimeError("ADDREV_TOKEN is not set")
    return {
        "Authorization": f"Bearer {ADDREV_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def addrev_get(path: str, params: dict | None = None):
    url = f"{ADDREV_BASE}{path}"
    r = requests.get(url, params=(params or {}), headers=_addrev_headers(), timeout=60)
    r.raise_for_status()
    data = r.json() or {}
    if isinstance(data, dict) and "results" in data:
        return data["results"] or []
    return data if isinstance(data, list) else []

def addrev_transactions(start_date: str, end_date: str, subrefs: list[str] | None = None,
                        contains: bool = False):
    params = {"fromDate": start_date, "toDate": end_date}
    if ADDREV_CHANNEL_ID:
        params["channelId"] = ADDREV_CHANNEL_ID
    rows = addrev_get("/transactions", params=params)

    want = [s.strip().lower() for s in (subrefs or []) if s.strip()]
    if want:
        def match(r):
            vals = []
            for k in ("clickRef", "clickref", "subId", "subid", "epi", "epi1", "epi2"):
                v = r.get(k)
                if v: vals.append(str(v))
            if not vals: return False
            low = "|".join(vals).lower()
            return any((w in low) for w in want) if contains else (set(want) & {v.lower() for v in vals})
        rows = [r for r in rows if match(r)]
    return rows

def addrev_commission_aggregate(start_date: str, end_date: str,
                                subrefs: list[str] | None = None, contains: bool = False,
                                target_ccy: str | None = None):
    rows = addrev_transactions(start_date, end_date, subrefs=subrefs, contains=contains)
    def detect_ccy(r):
        for k in ("currency", "currencyCode", "commissionCurrency"):
            if r.get(k): return str(r[k]).upper()
        return ADDREV_DEFAULT_CCY
    src_ccy = detect_ccy(rows[0]) if rows else ADDREV_DEFAULT_CCY
    tgt = (target_ccy or ADDREV_DEFAULT_CCY).upper()
    fx = get_fx_rate(src_ccy, tgt) if src_ccy != tgt else 1.0
    def get_amount(r):
        for k in ("commission", "publisherCommission", "reward", "amount", "value"):
            v = r.get(k)
            if v is None: continue
            try: return float(str(v).replace(",", ""))
            except: pass
        return 0.0
    confirmed = pending = 0.0
    for r in rows:
        status = str(r.get("status") or r.get("state") or "").lower()
        amt = get_amount(r)
        if status in ("approved", "confirmed", "paid"): confirmed += amt
        elif status in ("pending", "awaiting"): pending += amt
    return {
        "total_comm": (confirmed + pending) * fx,
        "confirmed_comm": confirmed * fx,
        "pending_comm": pending * fx,
        "raw": rows,
        "meta": {
            "source_currency": src_ccy,
            "target_currency": tgt,
            "fx_rate_used": fx,
            "window": f"{start_date} → {end_date}",
            "channelId": ADDREV_CHANNEL_ID or "",
        },
    }

# -------------------- Impact.com (optional third network) --------------------
IMPACT_ACCOUNT_SID = (os.getenv("IMPACT_ACCOUNT_SID") or "").strip()
IMPACT_AUTH_TOKEN  = (os.getenv("IMPACT_AUTH_TOKEN") or "").strip()
IMPACT_BASE_URL    = (os.getenv("IMPACT_BASE_URL") or "https://api.impact.com/Mediapartners").rstrip("/")
IMPACT_DEFAULT_CCY = (os.getenv("IMPACT_DEFAULT_CURRENCY") or "EUR").upper()


def _impact_configured() -> bool:
    return bool(IMPACT_ACCOUNT_SID and IMPACT_AUTH_TOKEN)


def impact_get(path: str, params: dict | None = None) -> dict:
    """
    Kald Impact partner API'et på en sikker måde.

    path: f.eks. "/Actions", "/Campaigns", "/Catalogs"
    Returnerer JSON-dict (eller {} hvis ikke konfigureret).
    """
    if not _impact_configured():
        return {}

    # Sørg for at path starter med '/'
    path = "/" + path.lstrip("/")

    # Korrekt URL jf. docs: /Mediapartners/<AccountSID>/<resource>
    # https://integrations.impact.com/.../list-actions-1
    url = f"{IMPACT_BASE_URL}/{IMPACT_ACCOUNT_SID}{path}"

    r = requests.get(
        url,
        params=params or {},
        auth=(IMPACT_ACCOUNT_SID, IMPACT_AUTH_TOKEN),
        headers={"Accept": "application/json"},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json() or {}
    return data if isinstance(data, dict) else {}


# -------- Impact earnings helper (samme struktur som AWIN/Addrevenue) --------
def impact_commission_aggregate(
    start_date: str,
    end_date: str,
    subrefs: list[str] | None = None,
    contains: bool = False,
    target_ccy: str | None = None,
):
    """
    Hent og aggreger commission fra Impact Actions API:
    GET /Mediapartners/:AccountSID/Actions

    Filtrerer evt. på SubId1/2/3 + SharedId, ligesom clickref/subid logik.
    """
    import datetime as _dt

    if not _impact_configured():
        return {
            "total_comm": 0.0,
            "confirmed_comm": 0.0,
            "pending_comm": 0.0,
            "raw": [],
            "meta": {"reason": "impact_not_configured"},
        }

    s = _dt.date.fromisoformat(start_date)
    e = _dt.date.fromisoformat(end_date)

    # Impact Actions: max 45 dage mellem start/end, men det overholder du allerede i UI
    start_iso = f"{s.isoformat()}T00:00:00Z"
    end_iso   = f"{e.isoformat()}T23:59:59Z"

    params = {
        "ActionDateStart": start_iso,
        "ActionDateEnd":   end_iso,
        "Page": 1,
        "PageSize": 20000,  # jf. docs, max 20.000 pr. side
    }

    all_actions: list[dict] = []

    while True:
        data = impact_get("/Actions", params=params)
        actions = data.get("Actions") or []
        if isinstance(actions, dict):
            actions = [actions]
        all_actions.extend(actions)

        # Pagination: brug @nextpageuri hvis sat
        next_uri = data.get("@nextpageuri") or data.get("@nextPageUri") or ""
        if not next_uri:
            break

        # Simpelt: bare øg Page – Impact begrænser selv til max ~10 sider
        params["Page"] = params.get("Page", 1) + 1
        if params["Page"] > 10:
            break  # safety, jf. docs anbefaling

    # --- Filter på SubIds / SharedId (clickref-agtigt) ---
    want = [s.strip() for s in (subrefs or []) if s.strip()]

    def _match(a: dict) -> bool:
        if not want:
            return True
        vals = []
        for k in ("SubId1", "SubId2", "SubId3", "SharedId", "PromoCode"):
            v = a.get(k)
            if v:
                vals.append(str(v))
        if not vals:
            return False

        if contains:
            low = " ".join(vals).lower()
            return any(w.lower() in low for w in want)
        else:
            lowset = {v.lower() for v in vals}
            wanted = {w.lower() for w in want}
            return bool(lowset & wanted)

    filtered = [a for a in all_actions if _match(a)]

    # --- Valuta og summering ---
    def _to_num(x):
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            try:
                return float(x.replace(",", "").strip())
            except Exception:
                return 0.0
        return 0.0

    src_ccy = IMPACT_DEFAULT_CCY
    for a in filtered[:3]:
        c = a.get("Currency")
        if c:
            src_ccy = str(c).upper()
            break

    tgt = (target_ccy or IMPACT_DEFAULT_CCY).upper()
    fx = get_fx_rate(src_ccy, tgt) if src_ccy != tgt else 1.0

    confirmed = pending = 0.0
    for a in filtered:
        payout = _to_num(a.get("Payout") or a.get("DeltaPayout") or 0.0)
        state = str(a.get("State") or "").upper()
        if state == "APPROVED":
            confirmed += payout
        elif state == "PENDING":
            pending += payout

    total = confirmed + pending

    return {
        "total_comm": total * fx,
        "confirmed_comm": confirmed * fx,
        "pending_comm": pending * fx,
        "raw": filtered,
        "meta": {
            "source_currency": src_ccy,
            "target_currency": tgt,
            "fx_rate_used": fx,
            "window": f"{s} → {e}",
            "rows_total": len(all_actions),
            "rows_after_filter": len(filtered),
            "subrefs_used": want,
            "contains_match": bool(contains),
            "used_api": "impact_actions",
        },
    }

# -------------------- Partnerize config + helper --------------------
import base64

PARTNERIZE_BASE = os.getenv(
    "PARTNERIZE_BASE",
    "https://api.partnerize.com"
).rstrip("/")

# App key (den her skal ALTID hedde sådan i .env)
PARTNERIZE_APP_KEY = (os.getenv("PARTNERIZE_APP_KEY") or "").strip()

# API key: accepter både PARTNERIZE_USER_API_KEY og PARTNERIZE_API_KEY
PARTNERIZE_API_KEY = (
    os.getenv("PARTNERIZE_USER_API_KEY")
    or os.getenv("PARTNERIZE_API_KEY")
    or ""
).strip()

PARTNERIZE_PUBLISHER_ID = (os.getenv("PARTNERIZE_PUBLISHER_ID") or "").strip()

# Partnerize: Partner ID = Publisher ID (per Partnerize support)
PARTNERIZE_PARTNER_ID = (os.getenv("PARTNERIZE_PARTNER_ID") or PARTNERIZE_PUBLISHER_ID or "").strip()

def partnerize_commission_aggregate(
    start_date: str,
    end_date: str,
    subrefs: list[str] | None = None,
    contains: bool = False,
    target_ccy: str | None = None,
):
    """
    Midlertidig stub for Partnerize earnings.
    Returnerer 0 på alle tal, så dashboardet ikke crasher, men du kan stadig
    få kampagner + feeds + tracking links i merchants-tabben.

    Hvis du senere vil have “rigtige” Partnerize-earnings, kan vi bygge den
    op omkring Reporting → Partner Conversions / Analytics (v3).
    """
    tgt = (target_ccy or (os.getenv("PREFERRED_CURRENCY") or "EUR")).upper()

    return {
        "total_comm": 0.0,
        "confirmed_comm": 0.0,
        "pending_comm": 0.0,
        "raw": [],
        "meta": {
            "reason": "partnerize_commission_not_implemented",
            "window": f"{start_date} → {end_date}",
            "subrefs_used": subrefs or [],
            "contains_match": bool(contains),
            "target_currency": tgt,
        },
    }

def _partnerize_configured() -> bool:
    """
    True hvis alle tre credentials er sat.
    Vi accepterer både PARTNERIZE_USER_API_KEY og PARTNERIZE_API_KEY.
    """
    return bool(
        PARTNERIZE_APP_KEY
        and PARTNERIZE_API_KEY
        and PARTNERIZE_PUBLISHER_ID
    )


def partnerize_get(path: str, params: dict | None = None) -> dict:
    """
    Generel Partnerize GET wrapper.
    Bruger Basic auth: base64(APP_KEY:API_KEY)
    og base-URL: https://api.partnerize.com
    """
    if not _partnerize_configured():
        return {}

    path = "/" + path.lstrip("/")
    url = f"{PARTNERIZE_BASE}{path}"

    # Basic auth-token: base64(APP_KEY:API_KEY)
    token = base64.b64encode(
        f"{PARTNERIZE_APP_KEY}:{PARTNERIZE_API_KEY}".encode("utf-8")
    ).decode("ascii")

    r = requests.get(
        url,
        params=params or {},
        headers={
            "Accept": "application/json",
            "Authorization": f"Basic {token}",
        },
        timeout=60,
    )

    if r.status_code == 401:
        raise RuntimeError(
            f"Partnerize 401 for {path} – check APP_KEY/API_KEY, "
            "PUBLISHER_ID og at nøglen har adgang til det endpoint "
            "(fx /user/publisher/{id}/feed)."
        )

    r.raise_for_status()
    data = r.json() or {}
    return data if isinstance(data, dict) else {}

import os
import time
import urllib.parse
import requests
import streamlit as st

# ----- 2Performant (affiliate) – login + programs + tracking links + feeds -----

TP_BASE = (os.getenv("TP_BASE") or "https://api.2performant.com").rstrip("/")
TP_USER_KEY = (os.getenv("TP_USER_KEY") or "").strip()
TP_EMAIL = (os.getenv("TP_EMAIL") or "").strip()
TP_PASSWORD = (os.getenv("TP_PASSWORD") or "").strip()


def _tp_configured() -> bool:
    """Er 2Performant sat op med env-var?"""
    return bool(TP_BASE and TP_USER_KEY and TP_EMAIL and TP_PASSWORD)


# 1) Liste over alle tilgængelige merchant product feeds
def tp_list_product_feeds(program_id: int | None = None, page: int = 1):
    """
    Wrapper omkring GET /affiliate/product_feeds

    Returnerer (product_feeds, metadata)
    """
    params = {"page": page}
    if program_id is not None:
        # 2Performant bruger filter[program_id] som query-param
        params["filter[program_id]"] = str(program_id)

    data = tp_get("/affiliate/product_feeds", params=params)
    return data.get("product_feeds", []), data.get("metadata", {})


# 2) Opret et nyt feed i din konto (som så får xml_link / csv_link)
DEFAULT_FEED_FIELDS = [
    "title",
    "url",
    "price",
    "old_price",
    "product_id",
    "gtin",
    "brand",
    "category",
    "image_urls",
    "description",
    "product_active",
    "created_at",
    "aff_code",
]


def tp_create_feed(name: str, tool_ids: list[int], fields: list[str] | None = None):
    """
    Wrapper omkring POST /affiliate/feeds.json

    name: et navn du selv vælger til feedet
    tool_ids: liste af product_feed IDs fra /affiliate/product_feeds
    fields: hvilke kolonner du vil have i dit feed
    """
    if fields is None:
        fields = DEFAULT_FEED_FIELDS

    payload = {
        "feed": {
            "name": name,
            "fields": fields,
            "tool_ids": tool_ids,
        }
    }

    data = tp_post("/affiliate/feeds.json", json_body=payload)
    return data.get("feed")


# 3) Hent dine feeds (dem der har xml_link / csv_link)
def tp_list_my_feeds(
    page: int = 1,
    perpage: int = 20,
    program_id: int | None = None,
):
    """
    Wrapper omkring GET /affiliate/feeds

    Returnerer (feeds, metadata)
    """
    params = {"page": page, "perpage": perpage}
    if program_id is not None:
        # iflg. feeds-endpoints: filter[program_id] kan bruges her
        params["filter[program_id]"] = str(program_id)

    data = tp_get("/affiliate/feeds", params=params)
    return data.get("feeds", []), data.get("metadata", {})


@st.cache_data(show_spinner=False, ttl=900)
def tp_feeds_for_program(program_id: int) -> list[dict]:
    """
    Hent alle affiliate-feeds for ET bestemt program via /affiliate/feeds?filter[program_id]=...

    Returnerer en liste af feed-objekter (hver har typisk xml_link og csv_link).
    """
    if not _tp_configured():
        return []

    try:
        feeds, _meta = tp_list_my_feeds(page=1, perpage=50, program_id=program_id)
    except Exception:
        return []

    if isinstance(feeds, dict):
        feeds = [feeds]
    return feeds or []

@st.cache_data(show_spinner=False, ttl=60)
def tp_bulk_create_missing_feeds(max_creations: int = 100) -> list[dict]:
    """
    Gennemløb alle affiliate-programmer og opret et produktfeed i /affiliate/feeds
    for de programmer, der:
      - har product_feeds_count > 0
      - men ingen eksisterende affiliate-feeds (tp_feeds_for_program == [])

    max_creations: hvor mange feeds vi maks. vil oprette i ét run
                   (for at undgå at spamme API'et helt vildt).

    Returnerer en liste med info om oprettede feeds.
    """
    created: list[dict] = []

    if not _tp_configured():
        st.error("2Performant er ikke konfigureret (TP_* mangler).")
        return created

    try:
        programs = tp_affiliate_programs()
    except Exception as e:
        st.error(f"Kunne ikke hente affiliate-programmer: {e}")
        return created

    if not programs:
        st.info("Ingen affiliate-programmer fundet i 2Performant.")
        return created

    created_count = 0

    for p in programs:
        if created_count >= max_creations:
            break

        pid = p.get("id")
        if not pid:
            continue

        feeds_count = p.get("product_feeds_count") or 0
        # spring programmer uden product feeds over
        if not feeds_count or feeds_count <= 0:
            continue

        # har vi allerede et affiliate-feed for dette program?
        existing_feeds = tp_feeds_for_program(int(pid))
        if existing_feeds:
            # vi skipper dem der allerede har feed (f.eks. 10qaroma)
            continue

        # hent "product_feeds" værktøjer for programmet
        try:
            product_feeds, _meta = tp_list_product_feeds(program_id=int(pid), page=1)
        except Exception:
            continue

        if not product_feeds:
            # der er registreret product_feeds_count, men vi kan ikke hente nogen tools
            continue

        tool_ids: list[int] = []
        for pf in product_feeds:
            pf_id = pf.get("id")
            if pf_id:
                tool_ids.append(int(pf_id))

        if not tool_ids:
            continue

        # navn til feed – relativt kort og genkendeligt
        prog_name = p.get("name") or p.get("slug") or f"program_{pid}"
        feed_name = f"{prog_name} – auto"

        try:
            feed = tp_create_feed(name=feed_name, tool_ids=tool_ids)
        except Exception as e:
            # vi logger bare fejlen i UI og går videre til næste program
            st.write(f"Kunne ikke oprette feed for program {pid} ({prog_name}): {e}")
            continue

        if not isinstance(feed, dict):
            continue

        created.append(
            {
                "program_id": pid,
                "program_name": prog_name,
                "feed_id": feed.get("id"),
                "feed_name": feed.get("name"),
                "feed_status": feed.get("status"),
                "xml_link": feed.get("xml_link"),
                "csv_link": feed.get("csv_link"),
            }
        )
        created_count += 1

    return created

def _tp_get_cached_tokens() -> dict | None:
    """
    Hent tokens fra session_state hvis de findes og ikke er udløbet.
    """
    tokens = st.session_state.get("_tp_tokens")
    if not tokens:
        return None

    expiry = tokens.get("expiry")
    if isinstance(expiry, (int, float)):
        # lidt buffer (60 sek) for ikke at ramme udløb midt i et kald
        if expiry > time.time() + 60:
            return tokens
        else:
            return None
    return tokens


def _tp_sign_in() -> dict:
    """
    POST /users/sign_in.json

    Headers:
      Content-Type: application/json
      user-key: <TP_USER_KEY>

    Body:
      { "user": { "email": "...", "password": "..." } }

    Returnerer token-headers + affiliate unique_code.
    """
    if not _tp_configured():
        raise RuntimeError(
            "2Performant env variables mangler (TP_BASE, TP_USER_KEY, TP_EMAIL, TP_PASSWORD)."
        )

    url = f"{TP_BASE}/users/sign_in.json"
    headers = {
        "Content-Type": "application/json",
        "user-key": TP_USER_KEY,
    }
    payload = {
        "user": {
            "email": TP_EMAIL,
            "password": TP_PASSWORD,
        }
    }

    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code == 401:
        raise RuntimeError(
            "2Performant login fejlede (401 Unauthorized) – tjek TP_EMAIL/TP_PASSWORD/TP_USER_KEY."
        )
    r.raise_for_status()

    data = {}
    try:
        data = r.json() or {}
    except Exception:
        pass

    user = data.get("user") or {}
    user_unique_code = user.get("unique_code") or ""  # bruges til quicklinks hvis vi vil

    h = r.headers
    try:
        expiry = int(h.get("expiry", "0"))
    except Exception:
        expiry = int(time.time()) + 3600

    tokens = {
        "access-token": h.get("access-token", ""),
        "client": h.get("client", ""),
        "uid": h.get("uid", TP_EMAIL),
        "token-type": h.get("token-type", "Bearer"),
        "expiry": expiry,
        "affiliate_unique_code": user_unique_code,
    }

    st.session_state["_tp_tokens"] = tokens
    return tokens


def _tp_auth_headers() -> dict:
    """
    Sørger for at vi har gyldige auth-headers (sign_in hvis nødvendigt).
    """
    if not _tp_configured():
        return {}

    tokens = _tp_get_cached_tokens()
    if not tokens:
        tokens = _tp_sign_in()

    return {
        "Content-Type": "application/json",
        "access-token": tokens["access-token"],
        "client": tokens["client"],
        "uid": tokens["uid"],
        "token-type": tokens.get("token-type", "Bearer"),
    }


def tp_get(path: str, params: dict | None = None) -> dict:
    """
    Generel GET-wrapper mod 2Performant med token-baseret auth.
    """
    if not _tp_configured():
        return {}

    if not path.startswith("/"):
        path = "/" + path
    url = f"{TP_BASE}{path}"

    headers = _tp_auth_headers()
    r = requests.get(url, headers=headers, params=params or {}, timeout=60)

    # token udløbet? prøv én gang mere efter login
    if r.status_code == 401:
        st.session_state.pop("_tp_tokens", None)
        headers = _tp_auth_headers()
        r = requests.get(url, headers=headers, params=params or {}, timeout=60)

    if r.status_code == 404:
        # ikke smadre hele UI'et pga. et forkert path
        st.warning(f"2Performant endpoint 404: {url}")
        return {}

    r.raise_for_status()

    try:
        data = r.json()
    except Exception:
        return {}

    return data if isinstance(data, dict) else {}


def tp_post(path: str, json_body: dict | None = None) -> dict | list:
    """
    Generel POST-wrapper mod 2Performant (bruges bl.a. til google_ads_linker og feeds).
    """
    if not _tp_configured():
        return {}

    if not path.startswith("/"):
        path = "/" + path
    url = f"{TP_BASE}{path}"

    headers = _tp_auth_headers()
    r = requests.post(url, headers=headers, json=json_body or {}, timeout=60)

    if r.status_code == 401:
        st.session_state.pop("_tp_tokens", None)
        headers = _tp_auth_headers()
        r = requests.post(url, headers=headers, json=json_body or {}, timeout=60)

    if r.status_code == 404:
        st.warning(f"2Performant endpoint 404: {url}")
        return {}

    r.raise_for_status()

    try:
        data = r.json()
    except Exception:
        return {}
    return data


@st.cache_data(show_spinner=False, ttl=1800)
def tp_affiliate_programs() -> list[dict]:
    """
    GET /affiliate/programs

    Vi henter alle affiliate-programmer (som din affiliate-bruger har relation til).
    """
    if not _tp_configured():
        return []

    all_programs: list[dict] = []
    page = 1

    while True:
        data = tp_get("/affiliate/programs", params={"page": page, "perpage": 50}) or {}
        programs = data.get("programs") or []

        if not programs:
            break

        all_programs.extend(programs)

        # pagination kan ligge enten som top-level "pagination" eller under metadata
        pagination = (
            data.get("pagination")
            or (data.get("metadata") or {}).get("pagination")
            or {}
        )
        current = pagination.get("current_page")
        pages = pagination.get("pages")

        if not current or not pages or current >= pages:
            break

        page += 1
        if page > 40:  # safety
            break

    return all_programs


@st.cache_data(show_spinner=False, ttl=1800)
def tp_affiliate_banner_for_program(program_id: int | str) -> dict | None:
    """
    Hent et enkelt banner for et program (for at få en 'link' tracking URL).

    GET /affiliate/banners?program_id=...
    """
    if not _tp_configured():
        return None

    data = tp_get(
        "/affiliate/banners",
        params={"program_id": program_id, "page": 1, "perpage": 1},
    ) or {}

    banners = data.get("banners") or []
    if isinstance(banners, dict):
        banners = [banners]
    if not banners:
        return None
    return banners[0]


def tp_quicklink_for_url(url: str) -> str:
    """
    Brug [Affiliate] Google Ads Linker Tracking Settings til at generere en quicklink-lignende tracking-URL.

    POST /affiliate/google_ads_linker/tracking_settings
      body: { "tracking_info": [ { "url": "<landing-url>" } ] }

    Svaret er en liste, hvor hvert element har:
      "tracking_url": "https://b-event.2performant.com/events/click?ad_type=quicklink&aff_code=...&unique=...&redirect_to={lpurl}&st=..."

    Vi erstatter {lpurl} med den rigtige, url-enkodede landing-URL så linket kan bruges direkte.
    """
    url = (url or "").strip()
    if not url:
        return ""

    try:
        data = tp_post(
            "/affiliate/google_ads_linker/tracking_settings",
            json_body={"tracking_info": [{"url": url}]},
        )
    except Exception:
        return ""

    if isinstance(data, dict):
        items = [data]
    else:
        items = data or []

    if not items:
        return ""

    first = items[0] or {}
    tracking_url = first.get("tracking_url") or ""
    if not tracking_url:
        return ""

    # erstat Google Ads placeholderen med en rigtig encoded URL
    if "{lpurl}" in tracking_url:
        encoded = urllib.parse.quote(url, safe="")
        tracking_url = tracking_url.replace("{lpurl}", encoded)

    return tracking_url

def tp_bulk_create_missing_feeds(max_creations: int = 100) -> list[dict]:
    """
    Gennemløb alle affiliate-programmer og opret et produktfeed i /affiliate/feeds
    for de programmer, der:
      - har product_feeds_count > 0
      - men ingen eksisterende affiliate-feeds (tp_feeds_for_program == [])

    max_creations: hvor mange feeds vi maks. vil oprette i ét run
                   (for at undgå at spamme API'et helt vildt).

    Returnerer en liste med info om oprettede feeds.
    """
    created: list[dict] = []

    if not _tp_configured():
        st.error("2Performant er ikke konfigureret (TP_* mangler).")
        return created

    try:
        programs = tp_affiliate_programs()
    except Exception as e:
        st.error(f"Kunne ikke hente affiliate-programmer: {e}")
        return created

    if not programs:
        st.info("Ingen affiliate-programmer fundet i 2Performant.")
        return created

    created_count = 0

    for p in programs:
        if created_count >= max_creations:
            break

        pid = p.get("id")
        if not pid:
            continue

        feeds_count = p.get("product_feeds_count") or 0
        # spring programmer uden product feeds over
        if not feeds_count or feeds_count <= 0:
            continue

        # har vi allerede et affiliate-feed for dette program?
        existing_feeds = tp_feeds_for_program(int(pid))
        if existing_feeds:
            # vi skipper dem der allerede har feed (f.eks. 10qaroma)
            continue

        # hent "product_feeds" værktøjer for programmet
        try:
            product_feeds, _meta = tp_list_product_feeds(program_id=int(pid), page=1)
        except Exception:
            continue

        if not product_feeds:
            # der er registreret product_feeds_count, men vi kan ikke hente nogen tools
            continue

        tool_ids: list[int] = []
        for pf in product_feeds:
            pf_id = pf.get("id")
            if pf_id:
                tool_ids.append(int(pf_id))

        if not tool_ids:
            continue

        # navn til feed – relativt kort og genkendeligt
        prog_name = p.get("name") or p.get("slug") or f"program_{pid}"
        feed_name = f"{prog_name} – auto"

        try:
            feed = tp_create_feed(name=feed_name, tool_ids=tool_ids)
        except Exception as e:
            # vi logger bare fejlen i UI og går videre til næste program
            st.write(f"Kunne ikke oprette feed for program {pid} ({prog_name}): {e}")
            continue

        if not isinstance(feed, dict):
            continue

        created.append(
            {
                "program_id": pid,
                "program_name": prog_name,
                "feed_id": feed.get("id"),
                "feed_name": feed.get("name"),
                "feed_status": feed.get("status"),
                "xml_link": feed.get("xml_link"),
                "csv_link": feed.get("csv_link"),
            }
        )
        created_count += 1

    return created


def render_2performant_merchants_table(country_code: str):
    """
    Viser dine 2Performant-programmer som affiliate med:

      - Program ID, navn, kategori, status
      - Land(e)
      - Produkter + product_feeds_count
      - Et banner tracking-link (hvis vi kan hente et banner)
      - En "Quicklink" tracking-URL til programmets main_url
      - XML/CSV feed URL pr. program, hvis der findes et affiliate-feed i /affiliate/feeds
      - (NYT) Knapp til at auto-oprette manglende produktfeeds via API'et
    """
    if not _tp_configured():
        st.info(
            "2Performant er ikke konfigureret – sæt TP_BASE, TP_USER_KEY, "
            "TP_EMAIL og TP_PASSWORD i Railway/.env."
        )
        return

    # --- NYT: UI til at oprette manglende produktfeeds ---
    with st.expander("Opret manglende produktfeeds i 2Performant", expanded=False):
        st.caption(
            "Vi scanner dine affiliate-programmer og opretter produktfeeds i /affiliate/feeds "
            "for de programmer, der har product_feeds_count > 0, men endnu ikke har et feed "
            "i din konto."
        )
        if st.button("Scan og opret manglende feeds", type="primary", key="tp_bulk_feeds_btn"):
            with st.spinner("Opretter manglende feeds via 2Performant API..."):
                created = tp_bulk_create_missing_feeds(max_creations=200)

            if not created:
                st.info(
                    "Ingen nye feeds blev oprettet.\n\n"
                    "Enten fandtes feed allerede for alle relevante programmer, "
                    "eller også havde de ingen product_feeds at bygge på."
                )
            else:
                st.success(f"Oprettede {len(created)} nye produktfeeds.")
                try:
                    st.dataframe(created, use_container_width=True)
                except Exception:
                    st.write(created)

    # --- Hent programmer som før ---
    try:
        programs = tp_affiliate_programs()
    except Exception as e:
        st.warning(f"2Performant /affiliate/programs fejlede: {e}")
        return

    if not programs:
        st.info(
            "2Performant API returnerede ingen programmer for denne affiliate-konto.\n\n"
            "Tjek at du er tilmeldt nogle programmer i 2Performant UI, "
            "og at credentials (TP_*) er korrekte."
        )
        return

    cc = (country_code or "").strip().upper()
    rows: list[dict] = []

    for p in programs:
        pid = p.get("id")
        if not pid:
            continue

        selling = p.get("selling_countries") or []
        if isinstance(selling, dict):
            selling = [selling]

        # filtrér på country code hvis brugt
        if cc and selling:
            country_codes = {
                str(c.get("code") or "").strip().upper()
                for c in selling
                if isinstance(c, dict)
            }
            if cc not in country_codes:
                continue

        name = p.get("name") or p.get("slug") or "(unknown)"
        main_url = p.get("main_url") or ""
        if not main_url and p.get("base_url"):
            main_url = "https://" + str(p.get("base_url")).lstrip("/")

        status = p.get("status") or ""
        category_name = (p.get("category") or {}).get("name", "")
        products_count = p.get("products_count") or 0
        feeds_count = p.get("product_feeds_count") or 0
        banners_count = p.get("banners_count") or 0
        payment_type = p.get("payment_type") or ""

        selling_str = ", ".join(
            sorted(
                {
                    (c.get("code") or c.get("name") or "").strip()
                    for c in selling
                    if isinstance(c, dict)
                }
            )
        )

        # Quicklink til programmets hoved-URL
        quicklink = ""
        if main_url:
            try:
                quicklink = tp_quicklink_for_url(main_url)
            except Exception:
                quicklink = ""

        # Banner tracking link (bare et eksempel-banner)
        banner_link = ""
        try:
            banner = tp_affiliate_banner_for_program(pid)
            if isinstance(banner, dict):
                banner_link = banner.get("link") or ""
        except Exception:
            banner_link = ""

        # XML/CSV feed-link fra /affiliate/feeds for netop dette program
        feed_xml = ""
        feed_csv = ""
        if feeds_count and feeds_count > 0:
            try:
                program_feeds = tp_feeds_for_program(int(pid))
                if program_feeds:
                    # prøv at vælge et aktivt feed først
                    active = [
                        f
                        for f in program_feeds
                        if str(f.get("status") or "").lower() == "active"
                    ]
                    feed = active[0] if active else program_feeds[0]
                    feed_xml = feed.get("xml_link") or ""
                    feed_csv = feed.get("csv_link") or ""
            except Exception:
                # hvis feeds-endpoint fejler, lader vi bare felterne være tomme
                pass

        rows.append(
            {
                "Program ID": pid,
                "Program": name,
                "Main URL": main_url,
                "Status": status,
                "Category": category_name,
                "Countries": selling_str,
                "Products": products_count,
                "Product feeds (count)": feeds_count,
                "Feed XML": feed_xml,
                "Feed CSV": feed_csv,
                "Banners (count)": banners_count,
                "Payment type": payment_type,
                "Banner tracking link": banner_link,
                "Quicklink to main URL": quicklink,
            }
        )

    if not rows:
        st.info(f"Ingen 2Performant-programmer matcher filteret for {cc or 'ALLE'}.")
        return

    # lidt emoji på status
    status_emoji = {
        "active": "🟢",
        "inactive": "⚪",
        "suspended": "🟠",
        "closed": "🔴",
    }
    for r in rows:
        s = str(r.get("Status") or "").strip().lower()
        emoji = status_emoji.get(s)
        if emoji:
            r["Status"] = f"{emoji} {r['Status']}"

    st.subheader(f"Merchants i {country_code or 'ALL'} • 2Performant (affiliate)")
    st.caption(
        "Data hentes fra 2Performant Affiliate API: /affiliate/programs, /affiliate/banners, "
        "/affiliate/google_ads_linker/tracking_settings og /affiliate/feeds.\n"
        "For hvert program med produkt-feeds forsøger vi at vise et XML- og CSV-feed "
        "fra dine egne affiliate-feeds. Du kan også auto-oprette manglende feeds i boksen ovenfor."
    )

    try:
        st.dataframe(
            rows,
            use_container_width=True,
            height=520,
            column_config={
                "Main URL": st.column_config.LinkColumn("Main URL"),
                "Banner tracking link": st.column_config.LinkColumn("Banner tracking link"),
                "Quicklink to main URL": st.column_config.LinkColumn("Quicklink to main URL"),
                "Feed XML": st.column_config.LinkColumn("Feed XML"),
                "Feed CSV": st.column_config.LinkColumn("Feed CSV"),
            },
        )
    except Exception:
        st.dataframe(rows, use_container_width=True, height=520)

# -------- Impact: Campaigns (programmer) + Catalog feeds --------
def impact_list_programs() -> list[dict]:
    """
    List alle programmer (campaigns) du er tilmeldt:
    GET /Mediapartners/:AccountSID/Campaigns?InsertionOrderStatus=Active

    Vi håndterer pagination via Page / PageSize + @nextpageuri.
    Returnerer en flad liste af campaign-objekter.
    """
    if not _impact_configured():
        return []

    params = {
        "InsertionOrderStatus": "Active",
        "Page": 1,
        "PageSize": 200,  # Impact begrænser typisk selv til 100, men det er fint
    }

    all_rows: list[dict] = []

    while True:
        data = impact_get("/Campaigns", params=params)
        rows = data.get("Campaigns") or []
        if isinstance(rows, dict):
            rows = [rows]
        all_rows.extend(rows)

        # pagination via @nextpageuri / @nextPageUri
        next_uri = data.get("@nextpageuri") or data.get("@nextPageUri") or ""
        if not next_uri:
            break

        params["Page"] = params.get("Page", 1) + 1
        if params["Page"] > 10:
            # safety break – du kan hæve dette, hvis du virkelig har 1000+ programmer
            break

    return all_rows

@st.cache_data(show_spinner=False, ttl=43200)
def impact_catalog_feeds_by_campaign() -> dict[str, list[str]]:
    """
    Slår /Catalogs op og bygger et map:
      { CampaignId(str) : [feed_urls,…] }

    Vi bruger primært ItemsUri som "feed API URL",
    plus evt. rå Locations hvis de findes.
    """
    if not _impact_configured():
        return {}

    feeds: dict[str, list[str]] = {}
    params = {"Page": 1, "PageSize": 200}

    while True:
        data = impact_get("/Catalogs", params=params)
        catalogs = data.get("Catalogs") or []
        if isinstance(catalogs, dict):
            catalogs = [catalogs]

        for c in catalogs:
            camp_id = str(c.get("CampaignId") or "").strip()
            if not camp_id:
                continue

            urls: list[str] = []

            # ItemsUri → sikker API-URL vi ved virker
            items_uri = c.get("ItemsUri")
            if isinstance(items_uri, str) and items_uri.strip():
                urls.append("https://api.impact.com" + items_uri.strip())

            # Locations: typisk direkte fil-paths (.txt.gz) – inkludér som ekstra info
            locs = c.get("Locations") or []
            if isinstance(locs, list):
                for loc in locs:
                    if isinstance(loc, str) and loc.strip():
                        urls.append(loc.strip())

            # dedupe
            uniq = []
            for u in urls:
                if u not in uniq:
                    uniq.append(u)

            if not uniq:
                continue

            feeds.setdefault(camp_id, [])
            for u in uniq:
                if u not in feeds[camp_id]:
                    feeds[camp_id].append(u)

        next_uri = data.get("@nextpageuri") or data.get("@nextPageUri") or ""
        if not next_uri:
            break
        params["Page"] = params.get("Page", 1) + 1
        if params["Page"] > 10:
            break

    return feeds

@st.cache_data(show_spinner=False, ttl=6*60*60)  # 6 timer
def cached_impact_programs():
    return impact_list_programs()

@st.cache_data(show_spinner=False, ttl=12*60*60)  # 12 timer
def cached_impact_catalog_feeds_by_campaign():
    return impact_catalog_feeds_by_campaign()

def render_impact_merchants_simple(country_code: str):
    """
    Impact merchants view filtered per country, following impact.com's recommendation:

    - Fetch all joined campaigns via /Campaigns (InsertionOrderStatus=Active)
    - Locally filter campaigns where:
        * ShippingRegions contains the selected country
        * AND either:
            - PrimaryRegion matches that market, OR
            - (if PrimaryRegion is missing) campaign Currency matches the expected currency
    """
    if not impact_simple_configured():
        st.info(
            "Impact.com is not configured – set IMPACT_ACCOUNT_SID and IMPACT_AUTH_TOKEN in .env."
        )
        return

    programs = impact_simple_programs()
    feeds_by_campaign = impact_catalog_feeds_by_campaign()

    if not programs:
        st.info("Impact API returned no campaigns for this account.")
        return

    cc = (country_code or "").strip().upper()

    # --- Country aliases: how Impact might spell the region names ---
    COUNTRY_ALIASES = {
        "IT": ["IT", "ITALY"],
        "SE": ["SE", "SWEDEN"],
        "DK": ["DK", "DENMARK"],
        "NO": ["NO", "NORWAY"],
        "FI": ["FI", "FINLAND"],
        "DE": ["DE", "GERMANY"],
        "FR": ["FR", "FRANCE"],
        "ES": ["ES", "SPAIN"],
        "NL": ["NL", "NETHERLANDS"],
        "BE": ["BE", "BELGIUM"],
        "PL": ["PL", "POLAND"],
        "UK": ["UK", "UNITEDKINGDOM", "GB"],
        "GB": ["GB", "UNITEDKINGDOM", "UK"],
        "US": ["US", "USA", "UNITEDSTATES"],
    }

    # --- Expected primary currency per market (best-effort) ---
    CURRENCY_BY_CC = {
        "IT": ["EUR"],
        "SE": ["SEK"],
        "DK": ["DKK"],
        "NO": ["NOK"],
        "FI": ["EUR"],
        "DE": ["EUR"],
        "FR": ["EUR"],
        "ES": ["EUR"],
        "NL": ["EUR"],
        "BE": ["EUR"],
        "PL": ["PLN"],
        "UK": ["GBP"],
        "GB": ["GBP"],
        "US": ["USD"],
    }

    def campaign_matches_market(c: dict) -> bool:
        # Hvis ingen country valgt, vis alt
        if not cc:
            return True

        # --- ShippingRegions check (kampagnen skal shippe til landet) ---
        regions = c.get("ShippingRegions") or []
        if isinstance(regions, str):
            regions = [regions]
        elif isinstance(regions, dict):
            maybe = regions.get("ShippingRegion") or regions.get("Region") or []
            if isinstance(maybe, list):
                regions = maybe
            else:
                regions = [maybe]

        regions_norm = {
            str(r).strip().upper().replace(" ", "")
            for r in regions
            if r is not None
        }

        wanted = {cc}
        if cc in COUNTRY_ALIASES:
            wanted.update(COUNTRY_ALIASES[cc])
        wanted_norm = {w.strip().upper().replace(" ", "") for w in wanted}

        if not (regions_norm & wanted_norm):
            # Campaign does not ship to this country at all
            return False

        # --- PrimaryRegion check (Impact's “home market” for the campaign) ---
        primary = (
            c.get("PrimaryRegion")
            or c.get("Primary Region")
            or c.get("primaryRegion")
            or c.get("primary_region")
        )

        primary_norm = (
            str(primary).strip().upper().replace(" ", "") if primary else ""
        )
        primary_matches = primary_norm in wanted_norm if primary_norm else False

        # --- Currency check (fallback if PrimaryRegion not set / unclear) ---
        cur = str(c.get("Currency") or "").upper().strip()
        allowed = CURRENCY_BY_CC.get(cc)

        # Hvis vi har en PrimaryRegion, brug den som “streng” indikator
        if primary_norm:
            # Kræv at PrimaryRegion matcher landet (IT / ITALY mv.)
            return primary_matches

        # Ellers, hvis ingen PrimaryRegion, brug kampagnens valuta som fallback
        if not allowed:
            # hvis vi ikke har en currency-mapping, accepterer vi den (kun ShippingRegions)
            return True

        return cur in allowed

    # Filter campaigns for the selected market
    filtered_programs = [p for p in programs if campaign_matches_market(p)]

    total_programs = len(programs)
    shown_programs = len(filtered_programs)

    if not filtered_programs:
        st.info(
            f"No Impact campaigns matched the filter for {cc}. "
            "Either no joined campaigns target this market, or PrimaryRegion/Currency "
            "is not set in a way that clearly indicates it."
        )
        return

    # ---------- Build rows for the table ----------
    rows = []
    for p in filtered_programs:
        camp_id = str(p.get("CampaignId") or "").strip()
        adv_id = p.get("AdvertiserId") or ""
        name = (
            p.get("CampaignName")
            or p.get("AdvertiserName")
            or "(unknown)"
        )
        status = p.get("ContractStatus") or ""
        tracking = p.get("TrackingLink") or ""
        currency = str(p.get("Currency") or "").upper().strip()
        shipping_regions = p.get("ShippingRegions") or []
        primary_region = (
            p.get("PrimaryRegion")
            or p.get("Primary Region")
            or p.get("primaryRegion")
            or p.get("primary_region")
            or ""
        )

        feed_urls = feeds_by_campaign.get(camp_id) or []
        feed_url = feed_urls[0] if feed_urls else ""

        rows.append(
            {
                "Advertiser ID": adv_id,
                "Campaign ID": camp_id,
                "Name": name,
                "Programme Status": status,
                "Currency": currency,
                "Primary Region": str(primary_region),
                "Shipping Regions": ", ".join(
                    [str(r) for r in shipping_regions]
                    if isinstance(shipping_regions, list)
                    else [str(shipping_regions)]
                ),
                "Feed CSV": feed_url,
                "Tracking deeplink": tracking,
            }
        )

    # Simple status decoration
    status_emoji = {
        "active": "🟢",
        "expired": "🔴",
    }
    for r in rows:
        s = str(r.get("Programme Status") or "").strip().lower()
        emoji = status_emoji.get(s, "")
        if emoji:
            r["Programme Status"] = f"{emoji} {r.get('Programme Status','')}".strip()

    st.subheader(f"Merchants • Impact.com ({cc or 'ALL'})")
    st.caption(
        (
            f"Showing {shown_programs} of {total_programs} joined campaigns for this Impact account, "
            f"filtered for {cc or 'all markets'} using ShippingRegions plus PrimaryRegion/Currency, "
            "following impact.com's recommendation. Results are best-effort: multi-market campaigns "
            "may still appear under several countries."
        )
    )

    try:
        st.dataframe(
            rows,
            use_container_width=True,
            height=520,
            column_config={
                "Advertiser ID": st.column_config.TextColumn(),
                "Campaign ID": st.column_config.TextColumn(),
                "Currency": st.column_config.TextColumn(),
                "Primary Region": st.column_config.TextColumn(),
                "Shipping Regions": st.column_config.TextColumn(),
                "Feed CSV": st.column_config.LinkColumn("Feed CSV"),
                "Tracking deeplink": st.column_config.LinkColumn("Tracking deeplink"),
            },
        )
    except Exception:
        st.dataframe(rows, use_container_width=True, height=520)

# -------------------- Email + alert helpers --------------------
def send_email(subject, body):
    if not ALERTS_ENABLED:
        return (False, "alerts disabled")
    needed = [SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, ALERT_TO, ALERT_FROM]
    if not all(needed):
        return (False, "SMTP not fully configured")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = ALERT_FROM
    msg["To"] = ALERT_TO
    msg.set_content(body)
    try:
        with smtplib.SMTP(SMTP_HOST, int(SMTP_PORT)) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
        return (True, "sent")
    except smtplib.SMTPAuthenticationError as e:
        return (False, f"auth failed: {e}")
    except Exception as e:
        return (False, f"send failed: {e}")

def alert_allowed(kind: str) -> bool:
    return (
        ALERTS_ENABLED and (
            (kind == "new"          and ALERT_ON_NEW) or
            (kind == "removed"      and ALERT_ON_REMOVED) or
            (kind == "closed"       and ALERT_ON_CLOSED) or
            (kind == "feed_failure" and ALERT_ON_FEED_FAILURE)
        )
    )

def log_alert(event: str, country: str, pid, name, details: str, email_res):
    ok, info = (email_res or (False, "not attempted"))
    with DB_LOCK:
        con = db(); cur = con.cursor()
        cur.execute(
            "INSERT INTO alert_log (ts, event, country, advertiser_id, name, details, email_sent, email_info) VALUES (?,?,?,?,?,?,?,?)",
            (dt.datetime.utcnow().isoformat(), event, country, pid, name, details, 1 if ok else 0, str(info)[:500])
        )
        con.commit(); con.close()

# -------------------- Alerts sync (AWIN programmes) --------------------
def sync_and_alert(country_code: str):
    now = dt.datetime.utcnow().isoformat()
    try:
        current = get_programmes(country_code)
    except Exception as e:
        if alert_allowed("feed_failure"):
            ts = dt.datetime.utcnow()
            last = FEED_ALERT_STATE.get(country_code)
            if not last or (ts - last).total_seconds() >= ALERT_COOLDOWN_MIN * 60:
                res = send_email(
                    f"[AWIN] Feed failure for {country_code}",
                    f"Fetching programmes failed for {country_code}\nError: {e}\nTime: {now}"
                )
                log_alert("feed_failure", country_code, None, None, f"error={e}", res)
                FEED_ALERT_STATE[country_code] = ts
        return

    seq = current if isinstance(current, list) else current.get("programmes", [])
    seen = {}
    for p in seq:
        adv_id = p.get("advertiserId") or p.get("programId") or p.get("id")
        if adv_id is None:
            continue
        seen[int(adv_id)] = {
            "name": p.get("advertiserName") or p.get("programName") or p.get("name") or "(unknown)",
            "status": p.get("programmeStatus") or p.get("status") or "",
            "relationship": p.get("relationship") or p.get("relationshipStatus") or ""
        }

    with DB_LOCK:
        con = db(); cur = con.cursor()
        cur.execute(
            "SELECT advertiser_id, name, status, relationship FROM programmes WHERE country=?",
            (country_code,)
        )
        previous = {row[0]: {"name": row[1], "status": row[2], "relationship": row[3]} for row in cur.fetchall()}

        # new
        for pid, v in seen.items():
            if pid not in previous:
                cur.execute(
                    """INSERT OR REPLACE INTO programmes
                       (advertiser_id, name, status, relationship, country, first_seen, last_seen)
                       VALUES (?,?,?,?,?,?,?)""",
                    (pid, v["name"], v["status"], v["relationship"], country_code, now, now)
                )
                if alert_allowed("new"):
                    res = send_email(
                        f"[AWIN] New programme: {v['name']}",
                        f"Country: {country_code}\nAdvertiser ID: {pid}\nStatus: {v['status']}\nRelationship: {v['relationship']}\nTime: {now}"
                    )
                    log_alert("new", country_code, pid, v["name"], f"status={v['status']} rel={v['relationship']}", res)

        # removed / changed
        closed_like = {"closed", "deactivated", "suspended"}
        for pid, prev in previous.items():
            v = seen.get(pid)
            if v is None:
                cur.execute("DELETE FROM programmes WHERE advertiser_id=? AND country=?", (pid, country_code))
                if alert_allowed("removed"):
                    res = send_email(
                        f"[AWIN] Programme removed: {prev['name']}",
                        f"Country: {country_code}\nAdvertiser ID: {pid}\nPrevious status: {prev['status']}\nPrevious relationship: {prev['relationship']}\nTime: {now}"
                    )
                    log_alert("removed", country_code, pid, prev["name"], f"prev_status={prev['status']} prev_rel={prev['relationship']}", res)
            else:
                if (v["status"] != prev["status"]) or (v["relationship"] != prev["relationship"]):
                    cur.execute(
                        """UPDATE programmes SET name=?, status=?, relationship=?, last_seen=?
                           WHERE advertiser_id=? AND country=?""",
                        (v["name"], v["status"], v["relationship"], now, pid, country_code)
                    )
                    if ((v["status"] or "").lower() in closed_like) or ((v["relationship"] or "").lower() in {"rejected", "suspended"}):
                        if alert_allowed("closed"):
                            res = send_email(
                                f"[AWIN] Programme closing: {v['name']} → {v['status']}/{v['relationship']}",
                                f"Country: {country_code}\nID: {pid}\nOld: {prev['status']} / {prev['relationship']}\nNew: {v['status']} / {v['relationship']}\nTime: {now}"
                            )
                            log_alert("closed", country_code, pid, v["name"], f"old={prev['status']}/{prev['relationship']} new={v['status']}/{v['relationship']}", res)
        con.commit(); con.close()

# -------------------- Earnings helpers (AWIN) --------------------
def advertiser_ids_for_countries(countries):
    ids = set()
    for cc in countries:
        try:
            progs = get_programmes(cc)
            seq = progs if isinstance(progs, list) else progs.get("programmes", [])
            for p in seq:
                adv_id = p.get("advertiserId") or p.get("programId") or p.get("id")
                if adv_id is not None:
                    ids.add(int(adv_id))
        except Exception:
            pass
    return ids

def get_earnings(region=None, start_date=None, end_date=None, tz="UTC"):
    """Aggregated earnings via Advertiser Performance (requires region as comma string)."""
    target_ccy = (os.getenv("PREFERRED_CURRENCY") or "EUR").upper()
    import datetime as _dt

    s = _dt.date.fromisoformat(start_date)
    e = _dt.date.fromisoformat(end_date)

    # Normalize region to a comma string like "FR,ES"
    if region is None:
        region = (os.getenv("AWIN_REGION") or "")
    if isinstance(region, (list, tuple, set)):
        region_str = ",".join(str(x) for x in region)
    else:
        region_str = str(region)
    region_str = (
        region_str.replace("[", "").replace("]", "")
                  .replace("'", "").replace('"', "")
    )
    region_list = [c.strip().upper() for c in region_str.split(",") if c.strip()]
    region_param = ",".join(region_list)

    params = {
        "accessToken": TOKEN,
        "startDate": s.isoformat(),
        "endDate":   e.isoformat(),
        "timezone": "UTC",
    }
    if region_param:
        params["region"] = region_param

    url = f"{API_BASE}/publishers/{PUB_ID}/reports/advertiser?{urlencode(params)}"
    r = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=60)
    if not r.ok:
        raise RuntimeError(f"{r.status_code} {r.reason}: {r.text}")

    data = r.json()
    rows = data["rows"] if isinstance(data, dict) and "rows" in data else (data if isinstance(data, list) else [])

    def to_num(x):
        if isinstance(x, (int, float)): return float(x)
        if isinstance(x, str):
            try: return float(x.replace(",", "").strip())
            except: return 0.0
        if isinstance(x, dict):
            for k in ("amount", "value", "val"):
                if k in x: return to_num(x[k])
        return 0.0

    confirmed = pending = total = 0.0
    for row in rows:
        confirmed += to_num(row.get("confirmedComm"))
        pending   += to_num(row.get("pendingComm"))
        total     += to_num(row.get("totalComm"))
    if total == 0.0 and (confirmed or pending):
        total = confirmed + pending

    src_ccy = "EUR"
    for probe in rows[:3]:
        src_ccy = (probe.get("currency") or probe.get("currencyCode") or src_ccy or "EUR")
    src_ccy = str(src_ccy).upper()

    fx = get_fx_rate(src_ccy, target_ccy)
    return {
        "total_comm": total * fx,
        "confirmed_comm": confirmed * fx,
        "pending_comm": pending * fx,
        "raw": rows,
        "meta": {
            "used_api": "advertiser_report",
            "source_currency": src_ccy,
            "target_currency": target_ccy,
            "fx_rate_used": fx,
            "regions_sent": region_param,
            "row_count": len(rows),
            "window": f"{s} → {e}",
            "clickrefs_used": [],
        },
    }

def get_commission_from_transactions(
    start_date,
    end_date,
    clickrefs=None,
    allowed_adv_ids=None,
    contains=False,
    date_type="transaction",
    status_filter=None
):
    import datetime as _dt
    s = _dt.date.fromisoformat(start_date)
    e = _dt.date.fromisoformat(end_date)
    if (e - s).days > 30:
        raise ValueError("Transactions API supports max 31 days. Reduce window (<=31).")
    start_dt = f"{s.isoformat()}T00:00:00Z"
    end_dt   = f"{e.isoformat()}T23:59:59Z"
    base_params = {
        "accessToken": TOKEN,
        "startDate": start_dt,
        "endDate": end_dt,
        "timezone": "UTC",
        "dateType": date_type,
    }
    ids = sorted(int(i) for i in (allowed_adv_ids or []))
    all_rows = []
    if not ids:
        params = dict(base_params); params["publisherIds"] = str(PUB_ID)
        url = f"{API_BASE}/publishers/{PUB_ID}/transactions?{urlencode(params)}"
        r = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=60)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            all_rows.extend(data)
        elif isinstance(data, dict) and "rows" in data:
            all_rows.extend(data.get("rows") or [])
    else:
        def chunks(lst, n=50):
            for i in range(0, len(lst), n):
                yield lst[i:i+n]
        for batch in chunks(ids, 50):
            params = dict(base_params)
            params["advertiserIds"] = ",".join(str(i) for i in batch)
            url = f"{API_BASE}/publishers/{PUB_ID}/transactions?{urlencode(params)}"
            r = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=60)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                all_rows.extend(data)
            elif isinstance(data, dict) and "rows" in data:
                all_rows.extend(data.get("rows") or [])
    total_rows = len(all_rows)

    want_refs = [c.strip() for c in (clickrefs or []) if c and c.strip()]
    if want_refs:
        if contains:
            wl = [w.lower() for w in want_refs]
        else:
            wl = {w.lower() for w in want_refs}
        def match_clickref(t):
            vals = [
                str(t.get(k))
                for k in ("clickRef","clickRef2","clickRef3","clickRef4","clickRef5","clickRef6")
                if t.get(k)
            ]
            if not vals: return False
            if contains:
                lowvals = [v.lower() for v in vals]
                return any(any(w in v for v in lowvals) for w in wl)
            else:
                lowset = {v.lower() for v in vals}
                return bool(lowset & wl)
    else:
        def match_clickref(t): return True

    filtered = [t for t in all_rows if match_clickref(t)]
    if status_filter:
        sf = str(status_filter).lower()
        filtered = [t for t in filtered if str(t.get("status","")).lower() == sf]

    def to_num(x):
        if isinstance(x, (int, float)): return float(x)
        if isinstance(x, str):
            try: return float(x.replace(",", "").strip())
            except: return 0.0
        if isinstance(x, dict):
            for k in ("amount", "value", "val"):
                if k in x: return to_num(x[k])
        return 0.0

    src_ccy = "EUR"
    for probe in filtered[:3]:
        src_ccy = (probe.get("currency") or probe.get("commissionCurrency") or src_ccy or "EUR")
    src_ccy = str(src_ccy).upper()
    target_ccy = (os.getenv("PREFERRED_CURRENCY") or "EUR").upper()
    fx = get_fx_rate(src_ccy, target_ccy)

    confirmed = pending = 0.0
    for t in filtered:
        status = (t.get("status") or "").lower()
        comm = (
            to_num(t.get("commissionAmount")) or
            to_num(t.get("commission")) or
            to_num(t.get("publisherCommission")) or
            0.0
        )
        if status == "approved": confirmed += comm
        elif status == "pending": pending += comm

    return {
        "total_comm": (confirmed + pending) * fx,
        "confirmed_comm": confirmed * fx,
        "pending_comm": pending * fx,
        "raw": filtered,
        "meta": {
            "used_api": "transactions",
            "source_currency": src_ccy,
            "target_currency": target_ccy,
            "fx_rate_used": fx,
            "window": f"{s} → {e}",
            "date_type": date_type,
            "status_filter": (status_filter or "none"),
            "clickrefs_used": want_refs,
            "rows_total": total_rows,
            "rows_after_clickref": len(filtered),
            "contains_match": bool(contains),
        },
    }

# -------------------- AWIN Feed + LinkBuilder helpers --------------------
def build_awin_feed_url(feed_id: str) -> str:
    key = AWIN_FEED_APIKEY
    fmt = (AWIN_FEED_FORMAT or "xml").strip().lower()
    lang = (AWIN_FEED_LANG or "en").strip()
    # Awin feed download pattern (fid + format + language)
    return f"https://datafeed.api.productserve.com/datafeed/download/apikey/{key}/fid/{feed_id}/format/{fmt}/language/{lang}"

@st.cache_data(show_spinner=False, ttl=43200, max_entries=1)
def load_awin_feed_map() -> dict[int, dict[str, str]]:
    """
    Returnerer et kompakt map:
      { advertiser_id: { REGION: feed_url, "_any": feed_url } }

    I stedet for at cache hele CSV’en som liste af dicts (RAM-tungt).
    """
    if not AWIN_FEED_APIKEY:
        return {}

    url = f"https://productdata.awin.com/datafeed/list/apikey/{AWIN_FEED_APIKEY}"
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
    except Exception:
        return {}

    text = r.text or ""

    # detect delimiter
    try:
        dialect = csv.Sniffer().sniff(text[:2048], delimiters=",;")
        delim = dialect.delimiter
    except Exception:
        delim = ","

    feed_map: dict[int, dict[str, str]] = {}
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    for raw in reader:
        d = {(k or "").strip(): (v or "").strip() for k, v in (raw or {}).items()}

        adv_id_raw = d.get("Advertiser ID") or d.get("advertiser id") or d.get("AdvertiserID") or ""
        try:
            adv_id = int(str(adv_id_raw))
        except Exception:
            continue

        region = (d.get("Primary Region") or d.get("primary region") or "").strip().upper()

        # Kun aktive/brugbare feeds (best effort)
        status = (d.get("Membership Status") or d.get("membership status") or "").strip().lower()
        # hvis du vil være hårdere: if status not in ("active","joined","approved"): continue

        # Find URL (brug den hvis den findes)
        feed_url = ""
        for k in ("Data feed download URL", "Download URL", "URL", "Url", "Datafeed URL"):
            v = d.get(k)
            if v:
                feed_url = v.strip()
                break

        # Ellers byg den fra feed id (sørg for at AWIN_FEED_FORMAT=xml i env)
        if not feed_url:
            feed_id = d.get("Feed ID") or d.get("feed id") or d.get("FeedID") or d.get("datafeed id")
            if feed_id:
                feed_url = build_awin_feed_url(str(feed_id).strip())

        if not feed_url:
            continue

        if adv_id not in feed_map:
            feed_map[adv_id] = {}

        # Gem pr region + en fallback
        if region and region not in feed_map[adv_id]:
            feed_map[adv_id][region] = feed_url
        feed_map[adv_id].setdefault("_any", feed_url)

    return feed_map

@st.cache_data(show_spinner=False, ttl=43200)
def cached_awin_tracking_link(advertiser_id: int, clickref: str | None = None) -> str:
    """Return a UI Link-Builder URL (works without a destination URL)."""
    cref = quote((clickref or "").strip())
    return f"https://ui.awin.com/affiliate/affiliate-tools/link-builder?awinmid={advertiser_id}&clickref={cref}"

# ----- Dognet (affiliate) – login + campaigns + tracking links + feeds + commissions -----

DOGNET_BASE = (os.getenv("DOGNET_BASE") or "https://api.app.dognet.com/api/v1").rstrip("/")
DOGNET_EMAIL = (os.getenv("DOGNET_EMAIL") or "").strip()
DOGNET_PASSWORD = (os.getenv("DOGNET_PASSWORD") or "").strip()
DOGNET_AD_CHANNEL_ID = (os.getenv("DOGNET_AD_CHANNEL_ID") or "").strip()

def _dognet_configured() -> bool:
    return bool(DOGNET_BASE and DOGNET_EMAIL and DOGNET_PASSWORD)

def _dognet_login() -> dict:
    if not _dognet_configured():
        return {}
    url = f"{DOGNET_BASE}/auth/login"
    r = requests.post(
        url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"email": DOGNET_EMAIL, "password": DOGNET_PASSWORD},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json() or {}

    token = data.get("token") or data.get("access_token") or (data.get("data") or {}).get("token")
    if not token:
        raise RuntimeError("Dognet login succeeded but response did not include token.")
    auth = {
        "token": token,
        "timezone": data.get("timezone") or (data.get("data") or {}).get("timezone"),
        "currency": (data.get("currency") or (data.get("data") or {}).get("currency") or "EUR").upper(),
        "obtained_at": time.time(),
    }
    st.session_state["_dognet_auth"] = auth
    return auth

def _dognet_auth() -> dict:
    if not _dognet_configured():
        return {}
    auth = st.session_state.get("_dognet_auth") or {}
    if not auth.get("token"):
        auth = _dognet_login()
    return auth

def _dognet_request(method: str, path: str, params: dict | None = None, json_body=None, retry401: bool = True):
    if not _dognet_configured():
        return {}
    if not path.startswith("/"):
        path = "/" + path
    url = f"{DOGNET_BASE}{path}"
    auth = _dognet_auth()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {auth.get('token','')}",
    }

    r = requests.request(method, url, headers=headers, params=params or {}, json=json_body, timeout=60)

    # token expired? retry once
    if r.status_code == 401 and retry401:
        st.session_state.pop("_dognet_auth", None)
        auth = _dognet_login()
        headers["Authorization"] = f"Bearer {auth.get('token','')}"
        r = requests.request(method, url, headers=headers, params=params or {}, json=json_body, timeout=60)

    # rate limit
    if r.status_code == 429:
        retry_after = int(r.headers.get("Retry-After") or "1")
        time.sleep(max(1, retry_after))
        return _dognet_request(method, path, params=params, json_body=json_body, retry401=retry401)

    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {}

def dognet_get(path: str, params: dict | None = None) -> dict:
    return _dognet_request("GET", path, params=params)

def dognet_post(path: str, json_body=None, params: dict | None = None) -> dict:
    return _dognet_request("POST", path, params=params, json_body=json_body)

@st.cache_data(show_spinner=False, ttl=6*60*60)
def dognet_ad_channels() -> list[dict]:
    if not _dognet_configured():
        return []
    data = dognet_get("/ad-channels") or {}
    rows = data.get("data") if isinstance(data, dict) else None
    if rows is None:
        # some APIs return list directly
        rows = data if isinstance(data, list) else []
    if isinstance(rows, dict):
        rows = [rows]
    return rows or []

def dognet_ad_channel_code(ad_channel_id: int | str) -> str:
    try:
        target = int(str(ad_channel_id))
    except Exception:
        return ""
    for ch in dognet_ad_channels():
        cid = ch.get("id") or ch.get("ad_channel_id")
        try:
            if int(str(cid)) == target:
                return str(ch.get("code") or ch.get("chid") or ch.get("ad_channel_code") or "").strip()
        except Exception:
            continue
    return ""

def dognet_campaigns_mine(ad_channel_id: int | str, status: int = 1) -> list[dict]:
    if not _dognet_configured():
        return []
    body = {
        "filter": [
            {"ad_channel_id": {"eq": int(str(ad_channel_id))}},
            {"ad_channel_in_campaign_status": {"eq": int(status)}},
        ]
    }
    data = dognet_post("/campaigns/mine/filter", json_body=body) or {}
    rows = data.get("data") if isinstance(data, dict) else None
    if rows is None:
        rows = data if isinstance(data, list) else []
    if isinstance(rows, dict):
        rows = [rows]
    return rows or []

def dognet_generate_link(ad_channel_id: int | str, campaign_id: int | str, url: str, data1: str = "", data2: str = "", url_type: int = 3) -> str:
    if not _dognet_configured():
        return ""
    body = {
        "ad_channel_id": int(str(ad_channel_id)),
        "campaign_id": int(str(campaign_id)),
        "url": url,
        "data1": data1 or "",
        "data2": data2 or "",
        "url_type": int(url_type),
    }
    data = dognet_post("/campaigns/links/generate", json_body=body) or {}
    # best-effort field detection
    if isinstance(data, dict):
        for k in ("link", "url", "affiliate_link", "tracking_url"):
            v = data.get(k)
            if v and str(v).startswith("http"):
                return str(v)
        inner = data.get("data") or {}
        if isinstance(inner, dict):
            for k in ("link", "url", "affiliate_link", "tracking_url"):
                v = inner.get(k)
                if v and str(v).startswith("http"):
                    return str(v)
    return ""

def _dognet_amount(r: dict) -> float:
    for k in (
        "publisher_commission",
        "commission",
        "commission_amount",
        "commission_value",
        "payout",
        "amount",
        "value",
        "price_commission",
    ):
        v = r.get(k)
        if v is None:
            continue
        try:
            return float(str(v).replace(",", "").strip())
        except Exception:
            pass
    return 0.0

def _dognet_status(r: dict) -> str:
    return str(r.get("rstatus") or r.get("status") or "").strip().upper()

def dognet_transactions_filter(start_dt: str, end_dt: str, per_page: int = 500, subrefs: list[str] | None = None, contains: bool = False):
    # returns list of rows using last_id scrolling (stop when data empty)
    if not _dognet_configured():
        return []

    flt = [
        {"rstatus": {"in": ["A", "P"]}},
        {"created_at": {"gte": start_dt}},
        {"created_at": {"lte": end_dt}},
    ]

    last_id = None
    out = []
    safety_pages = 0
    while True:
        body = {"filter": flt, "per-page": int(per_page)}
        if last_id is not None:
            body["last_id"] = last_id
        resp = dognet_post("/raw-transactions/filter", json_body=body) or {}
        data_rows = resp.get("data") if isinstance(resp, dict) else None
        if data_rows is None:
            data_rows = resp if isinstance(resp, list) else []
        if isinstance(data_rows, dict):
            data_rows = [data_rows]
        if not data_rows:
            break

        # client-side subref filtering (Dognet filter docs don't list subref fields)
        if subrefs:
            srefs = [str(x).strip() for x in subrefs if str(x).strip()]
            if srefs:
                def _hit(row: dict) -> bool:
                    fields = [
                        row.get("data1"), row.get("data2"),
                        row.get("subid"), row.get("sub_id"),
                        row.get("clickref"), row.get("click_ref"),
                        row.get("click_id"), row.get("clickid"),
                    ]
                    text = " ".join([str(v) for v in fields if v is not None])
                    if contains:
                        t = text.lower()
                        return any(s.lower() in t for s in srefs)
                    return any(s == str(v).strip() for s in srefs for v in fields if v is not None)
                data_rows = [r for r in data_rows if _hit(r)]

        out.extend(data_rows)

        meta = (resp.get("meta") if isinstance(resp, dict) else None) or {}
        last_id = meta.get("last_id")
        if last_id is None:
            mx = None
            for rr in data_rows:
                try:
                    rid = int(str(rr.get("id")))
                    mx = rid if mx is None else max(mx, rid)
                except Exception:
                    pass
            last_id = mx

        safety_pages += 1
        if safety_pages > 2000:
            break

    return out

def dognet_commission_aggregate(start_date: str, end_date: str, subrefs: list[str] | None = None, contains: bool = False, target_ccy: str | None = None) -> dict:
    if not _dognet_configured():
        return {"total_comm": 0.0, "confirmed_comm": 0.0, "pending_comm": 0.0, "raw": [], "meta": {}}

    start_dt = f"{start_date} 00:00:00"
    end_dt = f"{end_date} 23:59:59"

    rows = dognet_transactions_filter(start_dt, end_dt, per_page=500, subrefs=subrefs, contains=contains)

    auth = _dognet_auth()
    src_ccy = (auth.get("currency") or "EUR").upper()
    tgt = (target_ccy or src_ccy).upper()
    fx = get_fx_rate(src_ccy, tgt) if src_ccy != tgt else 1.0

    confirmed = pending = 0.0
    for r in rows:
        stt = _dognet_status(r)
        amt = _dognet_amount(r)
        if stt == "A":
            confirmed += amt
        elif stt == "P":
            pending += amt

    return {
        "total_comm": (confirmed + pending) * fx,
        "confirmed_comm": confirmed * fx,
        "pending_comm": pending * fx,
        "raw": rows,
        "meta": {
            "source_currency": src_ccy,
            "target_currency": tgt,
            "fx_rate_used": fx,
            "window": f"{start_date} → {end_date}",
            "timezone": auth.get("timezone") or "",
            "ad_channel_id": DOGNET_AD_CHANNEL_ID or "",
        },
    }

@st.cache_data(show_spinner=False, ttl=6*60*60)
def dognet_feeds() -> list[dict]:
    if not _dognet_configured():
        return []
    data = dognet_get("/campaigns/feeds") or {}
    rows = data.get("data") if isinstance(data, dict) else None
    if rows is None:
        rows = data if isinstance(data, list) else []
    if isinstance(rows, dict):
        rows = [rows]
    return rows or []

def render_dognet_merchants_table(country_code: str):
    st.subheader("Campaigns • Dognet")

    if not _dognet_configured():
        st.info("Dognet is selected, but DOGNET_EMAIL / DOGNET_PASSWORD are not configured in .env.")
        return

    if not DOGNET_AD_CHANNEL_ID:
        st.info("Tip: set DOGNET_AD_CHANNEL_ID in .env to list your approved campaigns and generate links.")
        return

    try:
        campaigns = dognet_campaigns_mine(DOGNET_AD_CHANNEL_ID, status=1)  # approved
    except Exception as e:
        st.error(f"Dognet campaigns fetch failed: {e}")
        return

    feed_rows = []
    try:
        feed_rows = dognet_feeds()
    except Exception:
        feed_rows = []
    feeds_by_campaign: dict[str, list[str]] = {}
    for f in feed_rows:
        cid = str(f.get("campaign_id") or f.get("campaignId") or "").strip()
        u = str(f.get("url") or f.get("feed_url") or f.get("link") or "").strip()
        if cid and u:
            feeds_by_campaign.setdefault(cid, []).append(u)

    norm = []
    for c in campaigns:
        cid = c.get("id") or c.get("campaign_id") or c.get("campaignId")
        name = c.get("name") or c.get("title") or c.get("campaign_name") or "(unknown)"
        url = c.get("url") or c.get("website") or c.get("landing_page") or ""
        cid_str = str(cid) if cid is not None else ""
        feed = ", ".join(feeds_by_campaign.get(cid_str, [])) if cid_str else ""
        norm.append({
            "Campaign ID": cid_str,
            "Name": name,
            "Website": url,
            "Feed": feed,
        })

    st.caption(
        "Shows approved campaigns for your configured Dognet ad channel. "
        "Feed URLs appear only when your account has access."
    )

    try:
        st.dataframe(
            norm,
            use_container_width=True,
            height=520,
            column_config={
                "Website": st.column_config.LinkColumn("Website"),
                "Feed": st.column_config.LinkColumn("Feed"),
            },
        )
    except Exception:
        st.dataframe(norm, use_container_width=True, height=520)

# -------------------- SIDEBAR (define inputs FIRST) --------------------
with st.sidebar:
    st.subheader("Filters")

      # Networks
    network_options = ["AWIN", "Addrevenue", "Impact", "Partnerize", "2Performant", "Dognet"]

    networks = st.multiselect(
        "Networks",
        options=network_options,
        default=network_options,
    )

    # AWIN filters
    country_input = st.text_input(
        "Country/ies (ISO-2, comma-separated)",
        value=os.getenv("AWIN_COUNTRY", COUNTRY)
    )
    region_input = st.text_input(
        "Region(s) for earnings (comma-separated ISO-2) — leave blank = use Country/ies",
        value=os.getenv("AWIN_REGION", "").strip()
    )

    # Time window
    days = st.slider("Days back (earnings window)", 1, 60, 5)

    # ClickRef filter
    clickrefs_input = st.text_input(
        "ClickRef / SubID filter (comma-separated; leave blank = all)",
        value=""
    )
    match_contains = st.checkbox("ClickRef match: contains (case-insensitive)", value=False)

    # Feed presence toggle
    show_with_feeds = st.checkbox("Show only programmes with product feeds", value=True)

    # Manual sync + alerts (AWIN)
    if st.button("Refresh now"):
        input_text = (country_input or "").strip()
        countries = [c.strip().upper() for c in input_text.split(",") if c.strip()]
        if not countries:
            countries = [os.getenv("AWIN_COUNTRY", COUNTRY)]
        errors = []
        with st.spinner("Syncing & checking alerts..."):
            for c in countries:
                try:
                    sync_and_alert(c)
                except Exception as e:
                    errors.append(f"{c}: {e}")
        if errors:
            st.error("Some countries failed: " + "; ".join(errors))
        else:
            st.success(f"Synced & checked alerts for: {', '.join(countries)}")

    if st.button("Clear cache (force reload)"):
        st.cache_data.clear()
        st.success("Cache cleared. Reloading…")
        st.rerun()
# ---------- Earnings panel (networks merged) ----------
end = dt.date.today()
start = end - dt.timedelta(days=days)


def _blank_metrics():
    return {
        "total_comm": 0.0,
        "confirmed_comm": 0.0,
        "pending_comm": 0.0,
        "raw": [],
        "meta": {},
    }


def _normalize_metrics(m):
    if not isinstance(m, dict):
        return {
            "total_comm": 0.0,
            "confirmed_comm": 0.0,
            "pending_comm": 0.0,
            "raw": (m if isinstance(m, list) else []),
            "meta": {},
        }
    for k in ("total_comm", "confirmed_comm", "pending_comm"):
        try:
            m[k] = float(m.get(k, 0.0) or 0.0)
        except Exception:
            m[k] = 0.0
    m.setdefault("raw", [])
    m.setdefault("meta", {})
    return m


# Byg liste over countries & clickrefs
countries_list = [c.strip().upper() for c in (country_input or "").split(",") if c.strip()]
if not countries_list:
    countries_list = [os.getenv("AWIN_COUNTRY", COUNTRY)]

clickrefs = [c.strip() for c in (clickrefs_input or "").split(",") if c.strip()]
use_awin_tx = bool(clickrefs)

# Forbered tomme metrics
awin_metrics = _blank_metrics()
addrev_metrics = _blank_metrics()
impact_metrics = _blank_metrics()
partnerize_metrics = _blank_metrics()
dognet_metrics = _blank_metrics()

# ----- AWIN -----
if "AWIN" in networks:
    try:
        if use_awin_tx:
            allowed_adv = advertiser_ids_for_countries(countries_list)
            if not allowed_adv:
                allowed_adv = None
            awin_metrics = get_commission_from_transactions(
                start.isoformat(),
                end.isoformat(),
                clickrefs=clickrefs,
                allowed_adv_ids=allowed_adv,
                contains=match_contains,
            )
        else:
            # ✅ Region is REQUIRED by AWIN advertiser report.
            # If user leaves region empty, fall back to countries_list.
            region_for_earnings = (region_input or "").strip()
            if not region_for_earnings:
                region_for_earnings = ",".join(countries_list)

            awin_metrics = get_earnings(
                region_for_earnings, start.isoformat(), end.isoformat(), tz="UTC"
            )
    except Exception as e:
        st.warning(f"AWIN earnings failed: {e}")
        awin_metrics = _blank_metrics()

# ----- Addrevenue -----
if "Addrevenue" in networks:
    try:
        addrev_metrics = addrev_commission_aggregate(
            start.isoformat(),
            end.isoformat(),
            subrefs=(clickrefs if clickrefs else None),
            contains=match_contains,
            target_ccy=(os.getenv("PREFERRED_CURRENCY") or "EUR"),
        )
    except Exception as e:
        st.warning(f"Addrevenue earnings failed: {e}")
        addrev_metrics = _blank_metrics()

# ----- Impact -----
if "Impact" in networks:
    try:
        impact_metrics = impact_commission_aggregate(
            start.isoformat(),
            end.isoformat(),
            subrefs=(clickrefs if clickrefs else None),
            contains=match_contains,
            target_ccy=(os.getenv("PREFERRED_CURRENCY") or "EUR"),
        )
    except Exception as e:
        # Vi vil stadig have AWIN/AdRev metrics, selv om Impact fejler
        st.warning(f"Impact earnings failed: {e}")
        impact_metrics = _blank_metrics()

# Normaliser og summer
awin_metrics = _normalize_metrics(awin_metrics)
addrev_metrics = _normalize_metrics(addrev_metrics)
impact_metrics = _normalize_metrics(impact_metrics)
partnerize_metrics = _normalize_metrics(partnerize_metrics)
dognet_metrics = _normalize_metrics(dognet_metrics)

partnerize_metrics = _blank_metrics()

# ----- Partnerize -----
if "Partnerize" in networks:
    if _partnerize_configured():
        try:
            partnerize_metrics = partnerize_commission_aggregate(
                start.isoformat(),
                end.isoformat(),
                target_ccy=(os.getenv("PREFERRED_CURRENCY") or "EUR"),
            )
        except Exception as e:
            st.warning(f"Partnerize earnings failed: {e}")
            partnerize_metrics = _blank_metrics()
    else:
        st.info(
            "Partnerize is selected, but Partnerize API credentials are not configured "
            "(PARTNERIZE_APP_KEY / PARTNERIZE_USER_API_KEY / PARTNERIZE_PUBLISHER_ID in .env)."
        )

# ----- Dognet -----
if "Dognet" in networks:
    if _dognet_configured():
        try:
            dognet_metrics = dognet_commission_aggregate(
                start.isoformat(),
                end.isoformat(),
                subrefs=(clickrefs if clickrefs else None),
                contains=match_contains,
                target_ccy=(os.getenv("PREFERRED_CURRENCY") or "EUR"),
            )
        except Exception as e:
            st.warning(f"Dognet earnings failed: {e}")
            dognet_metrics = _blank_metrics()
    else:
        st.info(
            "Dognet is selected, but Dognet API credentials are not configured "
            "(DOGNET_EMAIL / DOGNET_PASSWORD in .env)."
        )


total_comm = (
    awin_metrics["total_comm"]
    + addrev_metrics["total_comm"]
    + impact_metrics["total_comm"]
    + partnerize_metrics["total_comm"]
    + dognet_metrics["total_comm"]
)
confirmed_comm = (
    awin_metrics["confirmed_comm"]
    + addrev_metrics["confirmed_comm"]
    + impact_metrics["confirmed_comm"]
    + partnerize_metrics["confirmed_comm"]
    + dognet_metrics["confirmed_comm"]
)
pending_comm = (
    awin_metrics["pending_comm"]
    + addrev_metrics["pending_comm"]
    + impact_metrics["pending_comm"]
    + partnerize_metrics["pending_comm"]
    + dognet_metrics["pending_comm"]
)

ccy = (os.getenv("PREFERRED_CURRENCY") or "EUR").upper()
fmt = lambda x: f"{x:,.2f}"

c1, c2, c3 = st.columns(3)
c1.metric(f"Total commission ({ccy})", fmt(total_comm))
c2.metric(f"Confirmed commission ({ccy})", fmt(confirmed_comm))
c3.metric(f"Pending commission ({ccy})", fmt(pending_comm))

# -------------------- Addrevenue: feeds + tracking helpers --------------------
def _to_int_safe(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=43200, max_entries=1)
def addrev_feeds_by_advertiser():
    """
    Best-effort: hent alle product feeds og indeksér dem pr. advertiserId.
    Prøver /product-feeds først, falder tilbage til /feeds.
    Returnerer: dict[int -> list[dict]]  (hver dict kan have 'url', 'country', 'format' osv.)
    """
    if not ADDREV_TOKEN:
        return {}

    feeds = []
    # 1) forsøg primært endpoint
    try:
        feeds = addrev_get("/product-feeds", params={}) or []
    except Exception:
        feeds = []

    # 2) fallback
    if not feeds:
        try:
            feeds = addrev_get("/feeds", params={}) or []
        except Exception:
            feeds = []

    by_adv: dict[int, list] = {}
    for f in (feeds or []):
        adv = (
            f.get("advertiserId") or f.get("programId") or f.get("programmeId")
            or f.get("advertiser_id") or f.get("merchantId") or f.get("id")
        )
        adv_int = _to_int_safe(adv)
        if adv_int is None:
            continue

        # forsøg at finde en reel feed-URL
        url = (
            f.get("url") or f.get("feedUrl") or f.get("downloadUrl") or
            f.get("csvUrl") or f.get("xmlUrl")
        ) or ""

        # metainfo vi gerne vil vise hvis muligt
        country = f.get("country") or f.get("countryCode") or f.get("market") or ""
        fmt = f.get("format") or f.get("fileType") or ""

        item = {"url": url, "country": country, "format": fmt, **f}
        by_adv.setdefault(adv_int, []).append(item)

    return by_adv

def addrev_pick_feed_url(feeds_by_adv: dict, adv_id_int: int, country_code: str | None = None) -> str:
    """
    Vælg en enkelt feed-URL for en advertiser, helst i den ønskede country.
    """
    rows = feeds_by_adv.get(adv_id_int) or []
    if not rows:
        return ""
    if country_code:
        cc = country_code.strip().upper()
        candidates = [r for r in rows if str(r.get("country") or "").strip().upper() == cc]
        if candidates:
            # vælg første med non-empty URL
            for r in candidates:
                u = (r.get("url") or "").strip()
                if u:
                    return u
    # fallback: første non-empty URL
    for r in rows:
        u = (r.get("url") or "").strip()
        if u:
            return u
    return ""

def addrev_pick_tracking_link(p: dict) -> str:
    """
    Best-effort: find en tracking/deeplink i relations/advertiser-objektet.
    Ingen gættede patterns, kun felter fra API'et hvis de findes.
    """
    for k in ("trackingUrl", "clickUrl", "deeplink", "defaultDeeplink", "link", "url"):
        v = p.get(k)
        if v and str(v).strip().lower().startswith(("http://", "https://")):
            return str(v).strip()
    return ""

# -------- Addrevenue feeds & tracking links (per advertiser) --------

@st.cache_data(show_spinner=False, ttl=43200)
def addrev_product_feeds_by_adv(channel_id: str | int) -> dict[int, list[str]]:
    """
    Slår /productfeeds op og returnerer {advertiserId: [feed_urls,...]}.
    Vær defensiv ift. feltnavne, da API'et er i beta.
    """
    if not channel_id:
        return {}
    params = {"channelId": str(channel_id)}
    rows = addrev_get("/productfeeds", params=params)  # returns {"results":[...]} via addrev_get
    feeds_map: dict[int, list[str]] = {}
    for r in rows or []:
        # Gæt felter for annoncør-id
        adv = (
            r.get("advertiserId") or r.get("advertiser_id") or
            r.get("programId") or r.get("programmeId") or r.get("id")
        )
        try:
            adv_id = int(str(adv))
        except Exception:
            continue

        # Gæt felter for feed URL
        url_candidates = [
            r.get("feedUrl"), r.get("url"), r.get("downloadUrl"), r.get("downloadURL"),
            r.get("csvUrl"), r.get("xmlUrl")
        ]
        urls = [u for u in url_candidates if isinstance(u, str) and u.strip()]
        if not urls:
            # Nogle svar sender måske et nested objekt
            dl = r.get("download") or {}
            for k in ("csv", "xml", "url"):
                v = dl.get(k)
                if v and isinstance(v, str) and v.strip():
                    urls.append(v)

        if urls:
            feeds_map.setdefault(adv_id, [])
            # uden duplikater
            for u in urls:
                if u not in feeds_map[adv_id]:
                    feeds_map[adv_id].append(u)
    return feeds_map

@st.cache_data(show_spinner=False, ttl=43200)
def addrev_campaign_tracking_by_adv(channel_id: str | int) -> dict[int, str]:
    """
    Slår /campaigns?channelId=... op og vælger det første 'trackingLink' pr. advertiserId.
    Kun annoncører med godkendt relation returneres fra dette endpoint.
    """
    if not channel_id:
        return {}
    params = {"channelId": str(channel_id)}
    rows = addrev_get("/campaigns", params=params)
    link_map: dict[int, str] = {}
    for c in rows or []:
        adv = c.get("advertiserId") or c.get("advertiser_id") or c.get("id")
        try:
            adv_id = int(str(adv))
        except Exception:
            continue
        link = c.get("trackingLink") or c.get("tracking_url") or c.get("url")
        if isinstance(link, str) and link.strip():
            # tag første fundne pr. advertiser
            link_map.setdefault(adv_id, link.strip())
    return link_map

# -------------------- Helpers for relationship label --------------------
def _relationship_str(p: dict) -> str:
    """
    Return the publisher relationship for a programme, trying all known AWIN keys.
    Falls back to 'None' so the table never shows empty cells.
    """
    for k in (
        "relationship", "relationshipStatus", "partnershipStatus",
        "memberStatus", "membershipStatus", "relationStatus", "relation", "joinStatus"
    ):
        v = p.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return "None"

# -------------------- Merchants tables --------------------
def render_awin_merchants_table(
    country_code: str,
    feed_map: list[dict],
    first_clickref: str | None,
    only_with_feeds: bool = True,   # sidebar flag
):
    try:
        progs = cached_awin_programmes(country_code)
        seq = progs if isinstance(progs, list) else progs.get("programmes", [])
        if not isinstance(seq, list):
            seq = []

        rows = []
        for p in seq:
            status_val = p.get("programmeStatus") or p.get("status") or ""
            rel_val = _relationship_str(p)

            adv_id = p.get("advertiserId") or p.get("programId") or p.get("id")
            try:
                adv_id_int = int(adv_id) if adv_id is not None else 0
            except Exception:
                adv_id_int = 0

            # Feed URL (from preloaded feed list)
            feed_url = ""
            if feed_map and adv_id_int:
                m = feed_map.get(adv_id_int) or {}
                feed_url = m.get(country_code.strip().upper()) or m.get("_any") or ""

            # Proper tracking deeplink (cread.php)
            deeplink = awin_cread_link(adv_id_int, first_clickref, None)

            rows.append({
                "Advertiser ID": adv_id_int,
                "Name": p.get("advertiserName") or p.get("programName") or p.get("name"),
                "Programme Status": status_val,
                "Relationship": rel_val,
                "Feed XML": feed_url,
                "Tracking deeplink": deeplink,
            })

        # If feed list not available, auto-disable the feed filter so the table is never empty
        effective_only_with_feeds = only_with_feeds and bool(feed_map)
        if only_with_feeds and not feed_map:
            st.caption("Feed list unavailable or empty – feed filter disabled for this view.")

        # --- Feed presence filter ---
        # ON  -> keep rows that have BOTH Feed CSV and Tracking deeplink
        # OFF -> keep rows that have NO Feed CSV (deeplink may still exist)
        before_cnt = len(rows)
        if effective_only_with_feeds:
            rows = [
                r for r in rows
                if (str(r.get("Feed XML") or "").strip()
                    and str(r.get("Tracking deeplink") or "").strip())
            ]
        else:
            rows = [r for r in rows if not str(r.get("Feed XML") or "").strip()]
        after_cnt = len(rows)

        # Status emojis (decorate after filtering)
        status_emoji = {
            "active": "🟢", "open": "🟢",
            "accepted": "🟢", "approved": "🟢", "joined": "🟢",
            "closed": "🔴", "deactivated": "🔴", "rejected": "🔴", "declined": "🔴",
            "suspended": "🟠",
            "pending": "🟡", "awaiting": "🟡", "none": "⚪",
        }
        for r in rows:
            s = str(r.get("Programme Status") or "").strip().lower()
            rel = str(r.get("Relationship") or "").strip().lower()
            r["Programme Status"] = f"{status_emoji.get(s,'')} {r.get('Programme Status','')}".strip()
            r["Relationship"]     = f"{status_emoji.get(rel,'')} {r.get('Relationship','')}".strip()

        # Header + caption
        st.subheader(f"Merchants in {country_code} • AWIN")
        caption = (
            f"Feed filter: {'WITH feeds' if effective_only_with_feeds else 'WITHOUT feeds'} • "
            f"showing {after_cnt} of {len(seq)} programmes"
            + (f" (from {before_cnt} pre-filter)" if before_cnt != after_cnt else "")
            + ". This list comes from Awin's publisher programmes API and includes only "
            "programmes your publisher account has a relationship with "
            "(joined/approved/pending/rejected), not every possible programme in the market."
        )
        st.caption(caption)


        # Table
        try:
            st.dataframe(
                rows,
                use_container_width=True,
                height=520,
                column_config={
                    "Advertiser ID": st.column_config.NumberColumn(format="%d"),
                    "Feed XML": st.column_config.LinkColumn("Feed XML"),
                    "Tracking deeplink": st.column_config.LinkColumn("Tracking deeplink"),
                },
            )
        except Exception:
            st.dataframe(rows, use_container_width=True, height=520)

    except Exception as e:
        st.error(f"AWIN programmes fetch failed for {country_code}: {e}")

# ----- Addrevenue merchants (best-effort) -----
def addrev_list_advertisers(country_code: str | None):
    """
    Try to list advertisers/relations from Addrevenue. Prefer /relations; fall back to /advertisers.
    If no server-side country filter, fetch all and filter client-side.
    Returns (rows, used_path).
    """
    if not ADDREV_TOKEN:
        return [], ""

    try_paths = [
        ("/relations",   ("country", "countryCode", "region", "regionCode")),
        ("/advertisers", ("country", "countryCode", "region", "regionCode")),
    ]

    rows = []
    used_path = None

    # Try with a country param
    if country_code:
        for path, keys in try_paths:
            params = {keys[0]: country_code}
            try:
                rows = addrev_get(path, params=params)
                used_path = path
                if isinstance(rows, list):
                    break
            except Exception:
                rows = []
                continue

    # If not, try without params and filter locally
    if not rows:
        for path, _ in try_paths:
            try:
                rows = addrev_get(path, params={})
                used_path = path
                if isinstance(rows, list) and rows:
                    break
            except Exception:
                rows = []
                continue

    # Normalize
    norm = []
    for p in (rows or []):
        adv_id = (
            p.get("advertiserId") or p.get("programId") or p.get("id")
            or p.get("advertiser_id") or p.get("programmeId")
        )
        name = (
            p.get("advertiserName") or p.get("programName") or p.get("name")
            or p.get("title") or "(unknown)"
        )
        status = (
            p.get("programmeStatus") or p.get("status") or p.get("state")
            or p.get("relationStatus") or p.get("relation")
        )
        relationship = (
            p.get("relationship") or p.get("relationStatus") or p.get("relation")
            or p.get("status")
        )
        ctry = p.get("country") or p.get("countryCode") or p.get("region") or p.get("market")

        item = {
            "Advertiser ID": adv_id,
            "Name": name,
            "Programme Status": status,
            "Relationship": relationship,
            "Country": ctry,
        }

        if country_code and item["Country"]:
            if str(item["Country"]).strip().upper() != country_code.strip().upper():
                continue

        norm.append(item)

    return norm, (used_path or "")

@st.cache_data(show_spinner=False, ttl=12*60*60)  # 12 timer
def cached_addrev_list_advertisers(country_code: str | None):
    return addrev_list_advertisers(country_code)

def render_addrev_merchants_table(country_code: str):
    try:
        # Hent basisliste over advertisers/relations (din eksisterende helper)
        rows, used_path = cached_addrev_list_advertisers(country_code)

        # Hent feeds og tracking links pr. advertiser for din kanal (hvis sat)
        feeds_map = addrev_product_feeds_by_adv(ADDREV_CHANNEL_ID) if ADDREV_CHANNEL_ID else {}
        links_map = addrev_campaign_tracking_by_adv(ADDREV_CHANNEL_ID) if ADDREV_CHANNEL_ID else {}

        # Pynt + tilføj kolonner
        status_emoji = {
            "active":"🟢","open":"🟢","accepted":"🟢","approved":"🟢","joined":"🟢",
            "closed":"🔴","deactivated":"🔴","rejected":"🔴","declined":"🔴",
            "suspended":"🟠","pending":"🟡","awaiting":"🟡","none":"⚪"
        }

        norm_rows = []
        for r in rows:
            # normaliser felter
            try:
                adv_id_int = int(str(r.get("Advertiser ID")))
            except Exception:
                adv_id_int = None

            s = str(r.get("Programme Status") or "").strip().lower()
            rel = str(r.get("Relationship") or "").strip().lower()

            # læg emoji på
            r["Programme Status"] = f"{status_emoji.get(s,'')} {r.get('Programme Status','')}".strip()
            r["Relationship"]     = f"{status_emoji.get(rel,'')} {r.get('Relationship','')}".strip()

            # tilføj feed + tracking
            feed_list = feeds_map.get(adv_id_int, []) if adv_id_int is not None else []
            feed_val = ", ".join(feed_list) if feed_list else ""

            track_val = links_map.get(adv_id_int, "") if adv_id_int is not None else ""

            r["Feed CSV"] = feed_val
            r["Tracking deeplink"] = track_val

            norm_rows.append(r)

        st.subheader(f"Merchants in {country_code} • Addrevenue")

        count = len(norm_rows)
        base_caption = (
            f"Showing {count} merchants returned by Addrevenue for {country_code}. "
            "This list comes from Addrevenue's relations/advertisers API and includes only "
            "merchants your account has a relationship with (joined/approved/pending/rejected), "
            "not every possible merchant in the network."
        )
        if used_path:
            base_caption += f" (source endpoint: {used_path})"

        st.caption(base_caption)

        try:
            st.dataframe(
                norm_rows,
                use_container_width=True,
                height=520,
                column_config={
                    "Advertiser ID": st.column_config.NumberColumn(format="%d"),
                    "Feed CSV": st.column_config.LinkColumn("Feed CSV"),
                    "Tracking deeplink": st.column_config.LinkColumn("Tracking deeplink"),
                },
            )
        except Exception:
            st.dataframe(norm_rows, use_container_width=True, height=520)


        # lille debug note så du kan se om kanal-id mangler
        if not ADDREV_CHANNEL_ID:
            st.info("Tip: Sæt ADDREV_CHANNEL_ID i .env for at få produktfeeds og trackinglinks fra Addrevenue.")

    except Exception as e:
        st.error(f"Addrevenue programmes fetch failed for {country_code}: {e}")

# ----- Partnerize: participations + feeds + merchants table -----

# -------------------- Partnerize helpers --------------------

def partnerize_participations() -> list[dict]:
    """
    Henter kampagner/participations fra Partnerize og normaliserer dem til en
    samlet liste med ens struktur:

    {
      "campaign_id": str,
      "status": str,
      "default_currency": str,
      "promotional_countries": [ "DE", "DK", ... ],
      "campaign_info": {
          "title": str,
          "tracking_link": str,
      },
    }

    Bruger primært v3 /v3/partner/{publisherId}/participations
    og falder tilbage til v1 /user/publisher/{publisher_id}/campaign.
    """
    if not _partnerize_configured():
        return []

    all_norm: list[dict] = []

    @st.cache_data(show_spinner=False, ttl=12*60*60)  # 12 timer
    def cached_partnerize_participations():
        return partnerize_participations()


    # ---------- PRIMÆR: v3 /participations ----------
    try:
        page = 1
        page_size = 100

        while True:
            params = {
                "page": page,
                "page_size": page_size,
                "status": ["a", "p"],
                "campaign_status": ["a"],
}
        
            data = partnerize_get(
                f"/v3/partner/{PARTNERIZE_PARTNER_ID}/participations",
                params=params,
            ) or {}

            rows = data.get("data") or []
            if isinstance(rows, dict):
                rows = [rows]

            if not rows:
                break

            for p in rows:
                # NØGLEPUNKT: campaign_id: med kolon!
                cid = str(
                    p.get("campaign_id")
                    or p.get("campaign_id:")
                    or ""
                ).strip()
                if not cid:
                    continue

                status_raw = str(p.get("status") or "").strip().lower()
                default_ccy = str(
                    p.get("default_currency")
                    or p.get("default_currency:")
                    or ""
                ).upper()

                # campaign_info eller campaign_info:
                campaign_info = (
                    p.get("campaign_info")
                    or p.get("campaign_info:")
                    or {}
                )

                title = (
                    campaign_info.get("title")
                    or campaign_info.get("name")
                    or "(unknown)"
                )
                tracking = (
                    campaign_info.get("tracking_link")
                    or campaign_info.get("trackingLink")
                    or campaign_info.get("tracking_url")
                    or ""
                )

                # promotional_countries kan være liste, dict eller string
                promos = (
                    p.get("promotional_countries")
                    or p.get("promotional_countries:")
                    or []
                )
                promo_list: list[str] = []
                if isinstance(promos, dict):
                    promo_list = [str(k).upper() for k in promos.keys()]
                elif isinstance(promos, list):
                    promo_list = [str(x).upper() for x in promos]
                elif isinstance(promos, str):
                    promo_list = [promos.upper()]

                all_norm.append(
                    {
                        "campaign_id": cid,
                        "status": status_raw,
                        "default_currency": default_ccy,
                        "promotional_countries": promo_list,
                        "campaign_info": {
                            "title": title,
                            "tracking_link": tracking,
                        },
                    }
                )

            # simpel pagination: stop hvis vi får færre end page_size
            if len(rows) < page_size:
                break
            page += 1
            if page > 20:
                break  # safety

    except Exception:
        # Hvis v3 fejler helt, prøver vi v1 nedenfor
        all_norm = []

    if all_norm:
        return all_norm

    # ---------- FALBACK: v1 /user/publisher/{id}/campaign ----------
    try:
        v1_data = partnerize_get(
            f"/user/publisher/{PARTNERIZE_PUBLISHER_ID}/campaign",
            params={},
        ) or {}

        campaigns = v1_data.get("campaigns") or []
        if isinstance(campaigns, dict):
            campaigns = [campaigns]

        for item in campaigns:
            # v1 struktur: hver item har typisk { "campaign": {...}, ... }
            inner = item.get("campaign") or item

            cid = str(
                inner.get("campaign_id")
                or inner.get("campaign_id:")
                or inner.get("id")
                or ""
            ).strip()
            if not cid:
                continue

            status_raw = (
                str(inner.get("status") or item.get("status") or "")
                .strip()
                .lower()
            )
            default_ccy = str(
                inner.get("default_currency")
                or inner.get("currency")
                or ""
            ).upper()

            title = (
                inner.get("title")
                or inner.get("name")
                or "(unknown)"
            )

            tracking = (
                inner.get("tracking_link")
                or inner.get("tracking_url")
                or ""
            )

            # bedste gæt på lande-felt i v1:
            promo_raw = (
                inner.get("countries")
                or inner.get("country_codes")
                or item.get("countries")
                or []
            )
            promo_list: list[str] = []
            if isinstance(promo_raw, list):
                promo_list = [str(x).upper() for x in promo_raw]
            elif isinstance(promo_raw, dict):
                promo_list = [str(k).upper() for k in promo_raw.keys()]
            elif isinstance(promo_raw, str):
                promo_list = [promo_raw.upper()]

            all_norm.append(
                {
                    "campaign_id": cid,
                    "status": status_raw,
                    "default_currency": default_ccy,
                    "promotional_countries": promo_list,
                    "campaign_info": {
                        "title": title,
                        "tracking_link": tracking,
                    },
                }
            )

    except Exception:
        # Hvis v1 også fejler, returnerer vi tomt
        return []

    return all_norm

@st.cache_data(show_spinner=False, ttl=43200)
def partnerize_feeds_by_campaign() -> dict[str, list[str]]:
    """
    Slår Partnerize publisher feed API'et op og bygger map:
      { campaign_id (str): [feed_url, ...] }

    Endpoint jf. docs:
      GET /user/publisher/{publisher_id}/feed

    Vi begrænser til aktive feeds (active=y) og en moderat page_size
    for at undgå timeouts.
    """
    if not _partnerize_configured():
        return {}

    feeds_by_camp: dict[str, list[str]] = {}
    page = 1

    while True:
        params = {
            "page": page,
            "page_size": 50,
            "active": "y",
        }

        data = partnerize_get(
            f"/user/publisher/{PARTNERIZE_PUBLISHER_ID}/feed",
            params=params,
        ) or {}

        campaigns = data.get("campaigns") or []
        if isinstance(campaigns, dict):
            campaigns = [campaigns]

        if not campaigns:
            break

        for item in campaigns:
            camp = item.get("campaign") or item

            cid = str(camp.get("campaign_id") or camp.get("id") or "").strip()
            if not cid:
                continue

            feeds = camp.get("feeds") or camp.get("datafeeds") or []
            if isinstance(feeds, dict):
                feeds = [feeds]

            for f in feeds:
                if not isinstance(f, dict):
                    continue

                url_candidates = [
                    f.get("location"),
                    f.get("location_compressed"),
                    f.get("feed_url"),
                    f.get("download_url"),
                    f.get("url"),
                ]
                url = next((u for u in url_candidates if isinstance(u, str) and u.strip()), "")
                if not url:
                    continue

                feeds_by_camp.setdefault(cid, [])
                if url not in feeds_by_camp[cid]:
                    feeds_by_camp[cid].append(url)

        if len(campaigns) < params["page_size"]:
            break

        page += 1
        if page > 20:
            break
            
    return feeds_by_camp


def render_partnerize_merchants_table(country_code: str, only_with_feeds: bool = True):
    """
    Viser Partnerize-kampagner (participations) på samme måde som AWIN/Impact:
    - Kun kampagner du er tilmeldt
    - Filter på land vha. promotional_countries (hvis sat)
    - Feed CSV fra publisher feed API'et (hvis tilgængeligt)
    """
    if not _partnerize_configured():
        st.info(
            "Partnerize is not configured – set PARTNERIZE_APP_KEY, "
            "PARTNERIZE_USER_API_KEY and PARTNERIZE_PUBLISHER_ID in .env."
        )
        return

    programs = partnerize_participations()

    # Prøv at hente feeds; hvis det fejler, viser vi blot uden Feed CSV
    try:
        feeds_by_campaign = partnerize_feeds_by_campaign()
    except Exception as e:
        st.warning(
            f"Partnerize feed API failed ({e}); "
            "Campaigns are shown without Feed CSV for now."
        )
        feeds_by_campaign = {}

        # Auto-disable feed-filter hvis feeds ikke kan hentes / er tomt
    effective_only_with_feeds = only_with_feeds and bool(feeds_by_campaign)

    if only_with_feeds and not feeds_by_campaign:
        st.caption(
            "Partnerize feeds kunne ikke hentes eller er tomt – feed-filter er slået fra for denne visning."
        )

    if not programs:
        st.info("Partnerize API returned no participations for this account.")
        return

    cc = (country_code or "").strip().upper()
    rows = []

    for p in programs:
        cid = str(p.get("campaign_id") or "").strip()
        if not cid:
            continue

        status_raw = str(p.get("status") or "").strip().lower()
        default_ccy = str(p.get("default_currency") or "").upper()

        # Her er campaign_info allerede normaliseret i partnerize_participations()
        campaign_info = p.get("campaign_info") or {}
        title = (
            campaign_info.get("title")
            or campaign_info.get("name")
            or "(unknown)"
        )
        tracking = (
            campaign_info.get("tracking_link")
            or campaign_info.get("trackingLink")
            or campaign_info.get("tracking_url")
            or ""
        )

        # Promotional countries
        promos = p.get("promotional_countries") or []
        promo_list: list[str] = []
        if isinstance(promos, list):
            promo_list = [str(x).upper() for x in promos]
        elif isinstance(promos, dict):
            promo_list = [str(k).upper() for k in promos.keys()]
        elif isinstance(promos, str):
            promo_list = [promos.upper()]

        # Filter på valgt land
        if cc and promo_list:
            norm = {x.strip().upper() for x in promo_list}
            if cc not in norm:
                continue

        countries_str = (
            ", ".join(sorted({c.strip().upper() for c in promo_list if c}))
            if promo_list
            else ""
        )

        # Feed URLs (hvis feed-API'et er tilgængeligt)
        feed_urls = feeds_by_campaign.get(cid) if feeds_by_campaign else []
        feed_url = feed_urls[0] if feed_urls else ""

        rows.append(
            {
                "Campaign ID": cid,
                "Name": title,
                "Programme Status": status_raw,
                "Default Currency": default_ccy,
                "Promotional Countries": countries_str,
                "Feed CSV": feed_url,
                "Tracking deeplink": tracking,
            }
        )

    if not rows:
        st.info(f"No Partnerize campaigns matched the filter for {cc or 'ALL'}.")
        return

    # Evt. feed-filter som i AWIN
    before_cnt = len(rows)
    if effective_only_with_feeds:
        rows = [
            r for r in rows
            if str(r.get("Feed CSV") or "").strip()
        ]
    after_cnt = len(rows)

    # Status emojis
    status_emoji = {
        "active": "🟢",
        "approved": "🟢",
        "joined": "🟢",
        "pending": "🟡",
        "awaiting": "🟡",
        "rejected": "🔴",
        "declined": "🔴",
        "suspended": "🟠",
        "closed": "🔴",
        "none": "⚪",
    }
    for r in rows:
        s = str(r.get("Programme Status") or "").strip().lower()
        emoji = status_emoji.get(s, "")
        if emoji:
            r["Programme Status"] = f"{emoji} {r.get('Programme Status','')}".strip()

    st.subheader(f"Merchants in {country_code} • Partnerize")

    caption = (
        f"Feed filter: {'WITH feeds' if effective_only_with_feeds else 'ALL campaigns'} • "
        f"showing {after_cnt} of {before_cnt} joined campaigns. "
        "This list comes from Partnerize's participations API and includes only "
        "campaigns your publisher account has joined/has a relationship with, "
        "not every possible campaign in the network. "
        "Feed CSV is populated only if the Partnerize publisher feed API is enabled "
        "and accessible for your account."
    )
    st.caption(caption)

    try:
        st.dataframe(
            rows,
            use_container_width=True,
            height=520,
            column_config={
                "Campaign ID": st.column_config.TextColumn(),
                "Feed CSV": st.column_config.LinkColumn("Feed CSV"),
                "Tracking deeplink": st.column_config.LinkColumn("Tracking deeplink"),
            },
        )
    except Exception:
        st.dataframe(rows, use_container_width=True, height=520)

# ----- Impact merchants (programs) -----
def impact_list_programs():
    """
    Hent Impact-programmer (Campaigns) du er tilmeldt.
    """
    if not _impact_configured():
        return []
    url = f"{IMPACT_BASE_URL}/{IMPACT_ACCOUNT_SID}/Campaigns"
    params = {
        "InsertionOrderStatus": "Active",
    }
    r = requests.get(
        url,
        params=params,
        auth=(IMPACT_ACCOUNT_SID, IMPACT_AUTH_TOKEN),
        headers={"Accept": "application/json"},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json() or {}
    return data.get("Campaigns") or []

# -------------------- Simple Impact merchants (programs + catalogs) --------------------
IMPACT_ACCOUNT_SID_SIMPLE = (os.getenv("IMPACT_ACCOUNT_SID") or "").strip().strip("<>")
IMPACT_AUTH_TOKEN_SIMPLE  = (os.getenv("IMPACT_AUTH_TOKEN") or "").strip()
_IMPACT_BASE_RAW          = (os.getenv("IMPACT_BASE_URL") or "https://api.impact.com/Mediapartners").strip()

# Normaliser base URL til præcis .../Mediapartners
if "Mediapartners" in _IMPACT_BASE_RAW:
    head, _, _ = _IMPACT_BASE_RAW.partition("Mediapartners")
    IMPACT_BASE_URL_SIMPLE = (head + "Mediapartners").rstrip("/")
else:
    IMPACT_BASE_URL_SIMPLE = _IMPACT_BASE_RAW.rstrip("/")


def impact_simple_configured() -> bool:
    return bool(IMPACT_ACCOUNT_SID_SIMPLE and IMPACT_AUTH_TOKEN_SIMPLE)


def impact_simple_get(path: str, params: dict | None = None) -> dict:
    """
    Simpelt wrapper til Impact partner API:
      https://api.impact.com/Mediapartners/{AccountSID}/{path}
    """
    if not impact_simple_configured():
        return {}
    path = "/" + path.lstrip("/")
    url = f"{IMPACT_BASE_URL_SIMPLE}/{IMPACT_ACCOUNT_SID_SIMPLE}{path}"

    r = requests.get(
        url,
        params=params or {},
        auth=(IMPACT_ACCOUNT_SID_SIMPLE, IMPACT_AUTH_TOKEN_SIMPLE),
        headers={"Accept": "application/json"},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json() or {}
    return data if isinstance(data, dict) else {}


def impact_simple_programs() -> list[dict]:
    """
    Hent alle programmer (Campaigns), du er tilmeldt som mediepartner.
    """
    if not impact_simple_configured():
        return []

    params = {
        "InsertionOrderStatus": "Active",
        "Page": 1,
        "PageSize": 200,
    }

    all_rows: list[dict] = []

    while True:
        data = impact_simple_get("Campaigns", params=params)
        rows = data.get("Campaigns") or []
        if isinstance(rows, dict):
            rows = [rows]
        all_rows.extend(rows)

        next_uri = data.get("@nextpageuri") or data.get("@nextPageUri") or ""
        if not next_uri:
            break

        params["Page"] = params.get("Page", 1) + 1
        if params["Page"] > 10:
            break  # safety

    return all_rows
    
    @st.cache_data(show_spinner=False, ttl=6*60*60)  # 6 timer
    def cached_impact_programs():
        return impact_simple_programs()

    """
    Hent Catalogs og byg et map:
      { CampaignId (str): [feed_urls,...] }

    Vi bruger både ItemsUri (API) og Locations (filer) som 'Feed CSV'-links.
    """
    if not impact_simple_configured():
        return {}

    feeds: dict[str, list[str]] = {}
    params = {"Page": 1, "PageSize": 200}

    while True:
        data = impact_simple_get("Catalogs", params=params)
        catalogs = data.get("Catalogs") or []
        if isinstance(catalogs, dict):
            catalogs = [catalogs]

        for c in catalogs:
            camp_id = str(c.get("CampaignId") or "").strip()
            if not camp_id:
                continue

            urls: list[str] = []

            # ItemsUri → API endpoint for produkter
            items_uri = c.get("ItemsUri") or c.get("ItemsURI")
            if isinstance(items_uri, str) and items_uri.strip():
                uri = items_uri.strip()
                if uri.startswith("http"):
                    urls.append(uri)
                else:
                    urls.append("https://api.impact.com" + uri)

            # Locations → ofte direkte feed-filer (.txt.gz/.csv osv.)
            locs = c.get("Locations") or []
            if isinstance(locs, list):
                for loc in locs:
                    if isinstance(loc, str) and loc.strip():
                        urls.append(loc.strip())

            # dedupe
            uniq: list[str] = []
            for u in urls:
                if u not in uniq:
                    uniq.append(u)
            if not uniq:
                continue

            feeds.setdefault(camp_id, [])
            for u in uniq:
                if u not in feeds[camp_id]:
                    feeds[camp_id].append(u)

        next_uri = data.get("@nextpageuri") or data.get("@nextPageUri") or ""
        if not next_uri:
            break

        params["Page"] = params.get("Page", 1) + 1
        if params["Page"] > 10:
            break

    return feeds

def render_impact_merchants_simple(country_code: str):
    """
    Simple Impact merchants view:
    - Shows ALL campaigns returned by Impact for this account
    - Adds Feed CSV if Catalogs exist
    - Not filtered per country (Impact is global per account)
    """
    if not impact_simple_configured():
        st.info(
            "Impact.com is not configured – set IMPACT_ACCOUNT_SID and IMPACT_AUTH_TOKEN in .env."
        )
        return

    programs = cached_impact_programs()
    feeds_by_campaign = cached_impact_catalog_feeds_by_campaign()

    if not programs:
        st.info("Impact API returned no campaigns for this account.")
        return

    rows = []
    for p in programs:
        camp_id = str(p.get("CampaignId") or "").strip()
        adv_id = p.get("AdvertiserId") or ""
        name = (
            p.get("CampaignName")
            or p.get("AdvertiserName")
            or "(unknown)"
        )
        status = p.get("ContractStatus") or ""
        tracking = p.get("TrackingLink") or ""

        feed_urls = feeds_by_campaign.get(camp_id) or []
        feed_url = feed_urls[0] if feed_urls else ""

        rows.append(
            {
                "Advertiser ID": adv_id,
                "Campaign ID": camp_id,
                "Name": name,
                "Programme Status": status,
                "Feed CSV": feed_url,
                "Tracking deeplink": tracking,
            }
        )

    # Simple status decoration
    status_emoji = {
        "active": "🟢",
        "expired": "🔴",
    }
    for r in rows:
        s = str(r.get("Programme Status") or "").strip().lower()
        emoji = status_emoji.get(s, "")
        if emoji:
            r["Programme Status"] = f"{emoji} {r.get('Programme Status','')}".strip()

    st.subheader("Merchants • Impact.com")
    st.caption(
        (
            f"Showing {len(rows)} campaigns returned by Impact for this account. "
            "This list comes directly from Impact's global 'joined programmes' API and "
            "is not filtered per country (unlike Awin). The same merchant can therefore "
            "appear under multiple country tabs, even if it only targets some markets."
        )
    )

    try:
        st.dataframe(
            rows,
            use_container_width=True,
            height=520,
            column_config={
                "Advertiser ID": st.column_config.TextColumn(),
                "Campaign ID": st.column_config.TextColumn(),
                "Feed CSV": st.column_config.LinkColumn("Feed CSV"),
                "Tracking deeplink": st.column_config.LinkColumn("Tracking deeplink"),
            },
        )
    except Exception:
        st.dataframe(rows, use_container_width=True, height=520)

# -------------------- Merchants tables (per country, per network) --------------------
# Build country list from sidebar
countries_list = [c.strip().upper() for c in (country_input or "").split(",") if c.strip()]
if not countries_list:
    countries_list = [os.getenv("AWIN_COUNTRY", COUNTRY)]

# Preload AWIN feed list once; pass to every tab
feed_map = load_awin_feed_map()

# First clickref for tracking link (AWIN)
try:
    first_clickref = (clickrefs[0] if clickrefs else "")
except Exception:
    first_clickref = ""

# Decide which network tabs to show under each country
net_tabs = []
if "AWIN" in networks:
    net_tabs.append("AWIN")
if "Addrevenue" in networks:
    net_tabs.append("Addrevenue")
if "Impact" in networks:
    net_tabs.append("Impact")
if "Partnerize" in networks:
    net_tabs.append("Partnerize")
if "2Performant" in networks:
    net_tabs.append("2Performant")
if "Dognet" in networks:
    net_tabs.append("Dognet")

def _render_country(cc: str):
    if len(net_tabs) > 1:
        sub = st.tabs(net_tabs)
        for idx, net in enumerate(net_tabs):
            with sub[idx]:
                if net == "AWIN":
                    render_awin_merchants_table(
                        cc,
                        feed_map,
                        first_clickref,
                        only_with_feeds=show_with_feeds,   # AWIN bruger stadig feed-filteret
                    )
                elif net == "Addrevenue":
                    render_addrev_merchants_table(cc)
                elif net == "Impact":
                    # Brug den simple Impact-visning (ingen feed-filter)
                    render_impact_merchants_simple(cc)
                elif net == "Partnerize":
                    render_partnerize_merchants_table(
                        cc,
                        only_with_feeds=show_with_feeds,
                    )
                elif net == "2Performant":
                    render_2performant_merchants_table(cc)
                elif net == "Dognet":
                    render_dognet_merchants_table(cc)
    else:
        if "AWIN" in networks:
            render_awin_merchants_table(
                cc,
                feed_map,
                first_clickref,
                only_with_feeds=show_with_feeds,
            )
        if "Addrevenue" in networks:
            render_addrev_merchants_table(cc)
        if "Impact" in networks:
            render_impact_merchants_simple(cc)
        if "Partnerize" in networks:
            render_partnerize_merchants_table(
                cc,
                only_with_feeds=show_with_feeds,
            )
        if "2Performant" in networks:
            render_2performant_merchants_table(cc)
        if "Dognet" in networks:
            render_dognet_merchants_table(cc)

if len(countries_list) > 1:
    country_tabs = st.tabs(countries_list)
    for idx, cc in enumerate(countries_list):
        with country_tabs[idx]:
            _render_country(cc)
else:
    _render_country(countries_list[0])

    # ---------- AWIN (across all selected countries) ----------
    if "AWIN" in networks:
        st.markdown("### AWIN")
        awin_hits = []
        for cc in countries_list:
            try:
                progs = get_programmes(cc)
                seq = progs if isinstance(progs, list) else progs.get("programmes", [])
                if not isinstance(seq, list):
                    seq = []
            except Exception:
                continue

            for p in seq:
                name = (p.get("advertiserName") or p.get("programName") or p.get("name") or "")
                adv_id = p.get("advertiserId") or p.get("programId") or p.get("id")
                try:
                    adv_id_int = int(adv_id) if adv_id is not None else 0
                except Exception:
                    adv_id_int = 0

                    feed_url = ""
                    if feed_map and adv_id_int:
                        best = find_best_feed_for_adv(feed_map, adv_id_int, cc)
                        if best:
                            feed_url = feed_url_from_row(best)

                    deeplink = awin_cread_link(adv_id_int, first_clickref, None)
                    awin_hits.append({
                        "Country": cc,
                        "Advertiser ID": adv_id_int,
                        "Name": name,
                        "Programme Status": p.get("programmeStatus") or p.get("status") or "",
                        "Relationship": _relationship_str(p),
                        "Feed XML": feed_url,
                        "Tracking deeplink": deeplink,
                    })

        if awin_hits:
            st.dataframe(
                awin_hits,
                use_container_width=True,
                height=420,
                column_config={
                    "Feed XML": st.column_config.LinkColumn("Feed XML"),
                    "Tracking deeplink": st.column_config.LinkColumn("Tracking deeplink"),
                },
            )
        else:
            st.caption("No AWIN matches.")

    # ---------- Addrevenue ----------
    if "Addrevenue" in networks:
        st.markdown("### Addrevenue")
        addrev_hits = []
        for cc in countries_list:
            rows, _ = addrev_list_advertisers(cc)
            for r in rows:
                name = str(r.get("Name") or "")
                if ql in name.lower():
                    addrev_hits.append({**r, "Country": cc})
        if addrev_hits:
            st.dataframe(addrev_hits, use_container_width=True, height=420)
        else:
            st.caption("No Addrevenue matches.")

    # ---------- Impact ----------
    if "Impact" in networks:
        st.markdown("### Impact")
        try:
            programs = impact_simple_programs()
            feeds_by_campaign = impact_simple_catalog_feeds()
        except Exception:
            programs = []
            feeds_by_campaign = {}

        impact_hits = []
        for p in programs or []:
            name = (p.get("CampaignName") or p.get("AdvertiserName") or "(unknown)")
            if ql in str(name).lower():
                camp_id = str(p.get("CampaignId") or "").strip()
                feed_urls = feeds_by_campaign.get(camp_id) or []
                impact_hits.append({
                    "Advertiser ID": p.get("AdvertiserId") or "",
                    "Campaign ID": camp_id,
                    "Name": name,
                    "Programme Status": p.get("ContractStatus") or "",
                    "Feed": (feed_urls[0] if feed_urls else ""),
                    "Tracking deeplink": p.get("TrackingLink") or "",
                })

        if impact_hits:
            st.dataframe(
                impact_hits,
                use_container_width=True,
                height=420,
                column_config={
                    "Feed": st.column_config.LinkColumn("Feed"),
                    "Tracking deeplink": st.column_config.LinkColumn("Tracking deeplink"),
                },
            )
        else:
            st.caption("No Impact matches.")

    # ---------- Partnerize ----------
    if "Partnerize" in networks:
        st.markdown("### Partnerize")
        try:
            programs = partnerize_participations()
            feeds_by_campaign = partnerize_feeds_by_campaign()
        except Exception:
            programs = []
            feeds_by_campaign = {}

        pz_hits = []
        for p in programs or []:
            info = p.get("campaign_info") or {}
            title = info.get("title") or info.get("name") or "(unknown)"
            if ql in str(title).lower():
                cid = str(p.get("campaign_id") or "").strip()
                feed_urls = feeds_by_campaign.get(cid) or []
                pz_hits.append({
                    "Campaign ID": cid,
                    "Name": title,
                    "Programme Status": p.get("status") or "",
                    "Feed": (feed_urls[0] if feed_urls else ""),
                    "Tracking deeplink": info.get("tracking_link") or "",
                })

        if pz_hits:
            st.dataframe(
                pz_hits,
                use_container_width=True,
                height=420,
                column_config={
                    "Feed": st.column_config.LinkColumn("Feed"),
                    "Tracking deeplink": st.column_config.LinkColumn("Tracking deeplink"),
                },
            )
        else:
            st.caption("No Partnerize matches.")

# -------------------- Alerts Log panel --------------------
st.caption(
    "Uses AWIN Publisher API (Programmes + Reports), Addrevenue API, "
    "Impact.com Partner API (Actions + Campaigns), and Partnerize Partner API "
    "(conversions + participations + feeds). Product feeds shown via AWIN Feed List CSV, "
    "Addrevenue feeds, Impact Catalogs and Partnerize publisher feeds."
)
