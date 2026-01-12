# AWIN Publisher Dashboard

A lightweight Streamlit app that:
- Shows earnings (confirmed + pending commissions) for a selected region
- Lists active merchants/programmes for a given country (ISO-2)
- Sends email alerts when a programme disappears/gets closed or when a new one appears

## 1) Prereqs
- Python 3.9+
- An AWIN Publisher API token (AWIN UI → API Credentials → Show token)
- SMTP credentials (e.g., Gmail App Password, SendGrid SMTP, Mailgun SMTP)

## 2) Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then edit it
```

Edit `.env` with:
- `AWIN_TOKEN`, `AWIN_PUBLISHER_ID`
- `AWIN_COUNTRY` (for programmes list), `AWIN_REGION` (for earnings report)
- SMTP settings + `ALERT_TO`/`ALERT_FROM`

## 3) Run
```bash
streamlit run app.py
```
The app opens at http://localhost:8501

## 4) Notes
- The background scheduler polls every 3 hours to detect programme changes and sends email alerts.
- If your earnings report or programmes fields differ slightly from this example, tweak the JSON key lookups in `get_earnings()` or `get_programmes()`.
- For production use, consider Docker or a VM service and switch email to a provider API.
