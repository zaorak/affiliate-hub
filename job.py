# job.py
import os
import json
import uuid
import datetime as dt
from pathlib import Path

import requests
import gspread
from dotenv import load_dotenv


# -------------------- Load env --------------------
load_dotenv()

# -------------------- Google Sheets client --------------------
def get_client():
    """
    Lokalt: bruger service_account.json ved siden af job.py
    Railway: brug GOOGLE_SERVICE_ACCOUNT_JSON env var (du kan sætte den som Variable)
    """
    # 1) Railway/prod: JSON i env
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if sa_json:
        sa = json.loads(sa_json)
        return gspread.service_account_from_dict(sa)

    # 2) Lokal: fil ved siden af job.py
    key_path = Path(__file__).with_name("service_account.json")
    if key_path.exists():
        return gspread.service_account(filename=str(key_path))

    raise FileNotFoundError(
        "Missing Google credentials. Provide GOOGLE_SERVICE_ACCOUNT_JSON env var "
        "or place service_account.json next to job.py"
    )


def ensure_header(ws, header):
    # hvis sheet er tomt, skriv header
    values = ws.get_all_values()
    if not values:
        ws.append_row(header, value_input_option="RAW")
        return
    # hvis første række ikke matcher header, så skriv header som ny top (valgfrit)
    if values[0] != header:
        # du kan vælge at skippe dette, men det er rart at sikre format
        ws.insert_row(header, 1)


def append_row(row):
    sheet_id = os.environ["SHEET_ID"]
    worksheet_name = os.getenv("WORKSHEET_NAME", "Earnings")

    gc = get_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)

    header = [
        "run_id",
        "run_at_utc",
        "window_start",
        "window_end",
        "days_back",
        "currency",
        "networks",
        "countries",
        "region",
        "clickrefs",
        "clickref_contains",

        "awin_total",
        "awin_confirmed",
        "awin_pending",
        "awin_rows",

        "addrev_total",
        "addrev_confirmed",
        "addrev_pending",
        "addrev_rows",

        "impact_total",
        "impact_confirmed",
        "impact_pending",
        "impact_rows",

        "partnerize_total",
        "partnerize_confirmed",
        "partnerize_pending",
        "partnerize_rows",

        "grand_total",
        "grand_confirmed",
        "grand_pending",

        "status",
        "error",
    ]
    ensure_header(ws, header)
    ws.append_row(row, value_input_option="RAW")


# -------------------- Shared helpers --------------------
def to_num(x):
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x.replace(",", "").strip())
        except Exception:
            return 0.0
    if isinstance(x, dict):
        for k in ("amount", "value", "val"):
            if k in x:
                return to_num(x[k])
    return 0.0


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


def blank_metrics():
    return {
        "total_comm": 0.0,
        "confirmed_comm": 0.0,
        "pending_comm": 0.0,
        "raw": [],
        "meta": {},
    }


def normalize_metrics(m):
    if not isinstance(m, dict):
        return blank_metrics()
    for k in ("total_comm", "confirmed_comm", "pending_comm"):
        try:
            m[k] = float(m.get(k, 0.0) or 0.0)
        except Exception:
            m[k] = 0.0
    m.setdefault("raw", [])
    m.setdefault("meta", {})
    return m


# -------------------- AWIN --------------------
API_BASE = "https://api.awin.com"
TOKEN   = os.getenv("AWIN_TOKEN")
PUB_ID  = os.getenv("AWIN_PUBLISHER_ID")

def awin_get_programmes(country_code: str):
    params = {"accessToken": TOKEN, "countryCode": country_code}
    url = f"{API_BASE}/publishers/{PUB_ID}/programmes"
    r = requests.get(url, params=params, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=30)
    r.raise_for_status()
    return r.json()

def advertiser_ids_for_countries(countries):
    ids = set()
    for cc in countries:
        try:
            progs = awin_get_programmes(cc)
            seq = progs if isinstance(progs, list) else progs.get("programmes", [])
            for p in seq:
                adv_id = p.get("advertiserId") or p.get("programId") or p.get("id")
                if adv_id is not None:
                    ids.add(int(adv_id))
        except Exception:
            pass
    return ids

def awin_get_earnings(region, start_date, end_date):
    """
    AWIN advertiser report aggregate (samme som i app.py)
    """
    target_ccy = (os.getenv("PREFERRED_CURRENCY") or "EUR").upper()

    s = dt.date.fromisoformat(start_date)
    e = dt.date.fromisoformat(end_date)

    if region is None:
        region = os.getenv("AWIN_REGION") or os.getenv("AWIN_COUNTRY") or "FR"

    if isinstance(region, (list, tuple, set)):
        region_str = ",".join(str(x) for x in region)
    else:
        region_str = str(region)

    region_str = region_str.replace("[", "").replace("]", "").replace("'", "").replace('"', "")
    region_list = [c.strip().upper() for c in region_str.split(",") if c.strip()]
    region_param = ",".join(region_list)

    params = {
        "accessToken": TOKEN,
        "startDate": s.isoformat(),
        "endDate": e.isoformat(),
        "timezone": "UTC",
    }
    if region_param:
        params["region"] = region_param

    url = f"{API_BASE}/publishers/{PUB_ID}/reports/advertiser"
    r = requests.get(url, params=params, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=60)
    if not r.ok:
        raise RuntimeError(f"{r.status_code} {r.reason}: {r.text}")

    data = r.json()
    rows = data["rows"] if isinstance(data, dict) and "rows" in data else (data if isinstance(data, list) else [])

    confirmed = pending = total = 0.0
    for row in rows:
        confirmed += to_num(row.get("confirmedComm"))
        pending += to_num(row.get("pendingComm"))
        total += to_num(row.get("totalComm"))
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
        "meta": {"row_count": len(rows), "source_currency": src_ccy, "target_currency": target_ccy, "fx_rate_used": fx},
    }

def awin_get_commission_from_transactions(
    start_date,
    end_date,
    clickrefs=None,
    allowed_adv_ids=None,
    contains=False,
    date_type="transaction",
    status_filter=None
):
    s = dt.date.fromisoformat(start_date)
    e = dt.date.fromisoformat(end_date)
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

    url = f"{API_BASE}/publishers/{PUB_ID}/transactions"

    if not ids:
        params = dict(base_params)
        r = requests.get(url, params=params, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=60)
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
            r = requests.get(url, params=params, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=60)
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
            vals = [str(t.get(k)) for k in ("clickRef","clickRef2","clickRef3","clickRef4","clickRef5","clickRef6") if t.get(k)]
            if not vals:
                return False
            if contains:
                lowvals = [v.lower() for v in vals]
                return any(any(w in v for v in lowvals) for w in wl)
            lowset = {v.lower() for v in vals}
            return bool(lowset & wl)
    else:
        def match_clickref(t): return True

    filtered = [t for t in all_rows if match_clickref(t)]
    if status_filter:
        sf = str(status_filter).lower()
        filtered = [t for t in filtered if str(t.get("status","")).lower() == sf]

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
        if status == "approved":
            confirmed += comm
        elif status == "pending":
            pending += comm

    return {
        "total_comm": (confirmed + pending) * fx,
        "confirmed_comm": confirmed * fx,
        "pending_comm": pending * fx,
        "raw": filtered,
        "meta": {"rows_total": total_rows, "rows_after_filter": len(filtered), "source_currency": src_ccy, "target_currency": target_ccy, "fx_rate_used": fx},
    }


# -------------------- Addrevenue --------------------
ADDREV_BASE = (os.getenv("ADDREV_BASE", "https://addrevenue.io/api/v2").rstrip("/"))
ADDREV_TOKEN = os.getenv("ADDREV_TOKEN")
ADDREV_DEFAULT_CCY = (os.getenv("ADDREV_DEFAULT_CURRENCY") or "EUR").upper()
ADDREV_CHANNEL_ID = os.getenv("ADDREV_CHANNEL_ID")

def addrev_headers():
    if not ADDREV_TOKEN:
        raise RuntimeError("ADDREV_TOKEN is not set")
    return {
        "Authorization": f"Bearer {ADDREV_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def addrev_get(path: str, params: dict | None = None):
    url = f"{ADDREV_BASE}{path}"
    r = requests.get(url, params=(params or {}), headers=addrev_headers(), timeout=60)
    r.raise_for_status()
    data = r.json() or {}
    if isinstance(data, dict) and "results" in data:
        return data["results"] or []
    return data if isinstance(data, list) else []

def addrev_transactions(start_date: str, end_date: str, subrefs=None, contains=False):
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

def addrev_commission_aggregate(start_date: str, end_date: str, subrefs=None, contains=False, target_ccy=None):
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
            if v is None:
                continue
            try:
                return float(str(v).replace(",", ""))
            except:
                pass
        return 0.0

    confirmed = pending = 0.0
    for r in rows:
        status = str(r.get("status") or r.get("state") or "").lower()
        amt = get_amount(r)
        if status in ("approved", "confirmed", "paid"):
            confirmed += amt
        elif status in ("pending", "awaiting"):
            pending += amt

    return {
        "total_comm": (confirmed + pending) * fx,
        "confirmed_comm": confirmed * fx,
        "pending_comm": pending * fx,
        "raw": rows,
        "meta": {"source_currency": src_ccy, "target_currency": tgt, "fx_rate_used": fx, "row_count": len(rows)},
    }


# -------------------- Impact --------------------
IMPACT_ACCOUNT_SID = (os.getenv("IMPACT_ACCOUNT_SID") or "").strip()
IMPACT_AUTH_TOKEN  = (os.getenv("IMPACT_AUTH_TOKEN") or "").strip()
IMPACT_BASE_URL    = (os.getenv("IMPACT_BASE_URL") or "https://api.impact.com/Mediapartners").rstrip("/")
IMPACT_DEFAULT_CCY = (os.getenv("IMPACT_DEFAULT_CURRENCY") or "EUR").upper()

def impact_configured():
    return bool(IMPACT_ACCOUNT_SID and IMPACT_AUTH_TOKEN)

def impact_get(path: str, params: dict | None = None) -> dict:
    if not impact_configured():
        return {}
    path = "/" + path.lstrip("/")
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

def impact_commission_aggregate(start_date: str, end_date: str, subrefs=None, contains=False, target_ccy=None):
    if not impact_configured():
        return blank_metrics()

    s = dt.date.fromisoformat(start_date)
    e = dt.date.fromisoformat(end_date)

    start_iso = f"{s.isoformat()}T00:00:00Z"
    end_iso   = f"{e.isoformat()}T23:59:59Z"

    params = {"ActionDateStart": start_iso, "ActionDateEnd": end_iso, "Page": 1, "PageSize": 20000}

    all_actions = []
    while True:
        data = impact_get("/Actions", params=params)
        actions = data.get("Actions") or []
        if isinstance(actions, dict):
            actions = [actions]
        all_actions.extend(actions)
        next_uri = data.get("@nextpageuri") or data.get("@nextPageUri") or ""
        if not next_uri:
            break
        params["Page"] = params.get("Page", 1) + 1
        if params["Page"] > 10:
            break

    want = [s.strip() for s in (subrefs or []) if s.strip()]

    def match(a: dict) -> bool:
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
        lowset = {v.lower() for v in vals}
        wanted = {w.lower() for w in want}
        return bool(lowset & wanted)

    filtered = [a for a in all_actions if match(a)]

    def to_float(x):
        if isinstance(x, (int, float)): return float(x)
        if isinstance(x, str):
            try: return float(x.replace(",", "").strip())
            except: return 0.0
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
        payout = to_float(a.get("Payout") or a.get("DeltaPayout") or 0.0)
        state = str(a.get("State") or "").upper()
        if state == "APPROVED":
            confirmed += payout
        elif state == "PENDING":
            pending += payout

    return {
        "total_comm": (confirmed + pending) * fx,
        "confirmed_comm": confirmed * fx,
        "pending_comm": pending * fx,
        "raw": filtered,
        "meta": {"rows_total": len(all_actions), "rows_after_filter": len(filtered), "source_currency": src_ccy, "target_currency": tgt, "fx_rate_used": fx},
    }


# -------------------- Partnerize (stub som i din app.py) --------------------
def partnerize_commission_aggregate(start_date, end_date, target_ccy=None):
    tgt = (target_ccy or (os.getenv("PREFERRED_CURRENCY") or "EUR")).upper()
    return {
        "total_comm": 0.0,
        "confirmed_comm": 0.0,
        "pending_comm": 0.0,
        "raw": [],
        "meta": {"reason": "partnerize_commission_not_implemented", "target_currency": tgt},
    }


# -------------------- Main job --------------------
def main():
    run_id = str(uuid.uuid4())
    run_at = dt.datetime.now(dt.UTC).isoformat()

    # window
    days_back = int(os.getenv("DAYS_BACK", "5"))
    end = dt.date.today()
    start = end - dt.timedelta(days=days_back)

    start_s = start.isoformat()
    end_s = end.isoformat()

    # filters
    countries = [c.strip().upper() for c in (os.getenv("AWIN_COUNTRY", "SE") or "").split(",") if c.strip()]
    region = os.getenv("AWIN_REGION", os.getenv("AWIN_COUNTRY", "SE"))

    clickrefs_input = os.getenv("CLICKREFS", "").strip()
    clickrefs = [c.strip() for c in clickrefs_input.split(",") if c.strip()]
    clickref_contains = os.getenv("CLICKREF_CONTAINS", "false").lower() == "true"

    preferred_ccy = (os.getenv("PREFERRED_CURRENCY") or "EUR").upper()

    networks_env = os.getenv("NETWORKS", "AWIN,Addrevenue,Impact,Partnerize")
    networks = [n.strip() for n in networks_env.split(",") if n.strip()]
    networks_str = ",".join(networks)

    status = "ok"
    err = ""

    awin_metrics = blank_metrics()
    addrev_metrics = blank_metrics()
    impact_metrics = blank_metrics()
    partnerize_metrics = blank_metrics()

    try:
        # AWIN
        if "AWIN" in networks:
            if clickrefs:
                allowed_adv = advertiser_ids_for_countries(countries) if countries else None
                awin_metrics = awin_get_commission_from_transactions(
                    start_s, end_s,
                    clickrefs=clickrefs,
                    allowed_adv_ids=(allowed_adv if allowed_adv else None),
                    contains=clickref_contains,
                )
            else:
                awin_metrics = awin_get_earnings(region, start_s, end_s)

        # Addrevenue
        if "Addrevenue" in networks and ADDREV_TOKEN:
            addrev_metrics = addrev_commission_aggregate(
                start_s, end_s,
                subrefs=(clickrefs if clickrefs else None),
                contains=clickref_contains,
                target_ccy=preferred_ccy,
            )

        # Impact
        if "Impact" in networks and impact_configured():
            impact_metrics = impact_commission_aggregate(
                start_s, end_s,
                subrefs=(clickrefs if clickrefs else None),
                contains=clickref_contains,
                target_ccy=preferred_ccy,
            )

        # Partnerize (stub)
        if "Partnerize" in networks:
            partnerize_metrics = partnerize_commission_aggregate(
                start_s, end_s, target_ccy=preferred_ccy
            )

        # normalize
        awin_metrics = normalize_metrics(awin_metrics)
        addrev_metrics = normalize_metrics(addrev_metrics)
        impact_metrics = normalize_metrics(impact_metrics)
        partnerize_metrics = normalize_metrics(partnerize_metrics)

    except Exception as e:
        status = "error"
        err = str(e)[:500]

    grand_total = awin_metrics["total_comm"] + addrev_metrics["total_comm"] + impact_metrics["total_comm"] + partnerize_metrics["total_comm"]
    grand_conf  = awin_metrics["confirmed_comm"] + addrev_metrics["confirmed_comm"] + impact_metrics["confirmed_comm"] + partnerize_metrics["confirmed_comm"]
    grand_pend  = awin_metrics["pending_comm"] + addrev_metrics["pending_comm"] + impact_metrics["pending_comm"] + partnerize_metrics["pending_comm"]

    row = [
        run_id,
        run_at,
        start_s,
        end_s,
        str(days_back),
        preferred_ccy,
        networks_str,
        ",".join(countries),
        str(region),
        ",".join(clickrefs),
        "true" if clickref_contains else "false",

        f"{awin_metrics['total_comm']:.6f}",
        f"{awin_metrics['confirmed_comm']:.6f}",
        f"{awin_metrics['pending_comm']:.6f}",
        str(len(awin_metrics.get("raw") or [])),

        f"{addrev_metrics['total_comm']:.6f}",
        f"{addrev_metrics['confirmed_comm']:.6f}",
        f"{addrev_metrics['pending_comm']:.6f}",
        str(len(addrev_metrics.get("raw") or [])),

        f"{impact_metrics['total_comm']:.6f}",
        f"{impact_metrics['confirmed_comm']:.6f}",
        f"{impact_metrics['pending_comm']:.6f}",
        str(len(impact_metrics.get("raw") or [])),

        f"{partnerize_metrics['total_comm']:.6f}",
        f"{partnerize_metrics['confirmed_comm']:.6f}",
        f"{partnerize_metrics['pending_comm']:.6f}",
        str(len(partnerize_metrics.get("raw") or [])),

        f"{grand_total:.6f}",
        f"{grand_conf:.6f}",
        f"{grand_pend:.6f}",

        status,
        err,
    ]

    append_row(row)
    print(f"[job] wrote earnings snapshot: status={status}")


if __name__ == "__main__":
    main()
