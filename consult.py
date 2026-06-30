import time
import requests
import pandas as pd
from io import StringIO
from pathlib import Path

TMP = Path("tmp/pdfs")
TMP.mkdir(parents=True, exist_ok=True)

BASE = "https://consult.cbso.nbb.be/api"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}


def make_session(enterprise_number: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    page_url = f"https://consult.cbso.nbb.be/consult-enterprise/{enterprise_number}"
    session.headers.update({"Referer": page_url})
    session.get(page_url)  # establishes ASLBSA, ASLBSACORS, JSESSIONID cookies
    return session


def get_deposits(session: requests.Session, enterprise_number: str) -> list:
    url = (
        f"{BASE}/rs-consult/published-deposits"
        f"?page=0&size=10&enterpriseNumber={enterprise_number}"
        f"&sort=periodEndDate,desc&sort=depositDate,desc"
    )
    r = session.get(url)
    r.raise_for_status()
    data = r.json()
    print(f"Found {data['totalElements']} filings ({data['totalPages']} pages). Loading first {len(data['content'])}.")
    return data["content"]


def download_csv(session: requests.Session, deposit_id: str) -> str:
    url = f"{BASE}/external/broker/public/deposits/consult/csv/{deposit_id}"
    r = session.get(url)
    r.raise_for_status()
    return r.text


def download_pdf(session: requests.Session, deposit: dict) -> Path:
    """Download PDF for a deposit and save to tmp/pdfs/. Returns the saved path."""
    deposit_id  = deposit["id"]
    year        = deposit["periodEndDateYear"]
    enterprise  = deposit["enterpriseNumber"]
    reference   = deposit["reference"]
    filename    = f"{enterprise}_{year}_{reference}.pdf"
    dest        = TMP / filename

    if dest.exists():
        print(f"    PDF already exists: {filename}")
        return dest

    url = f"{BASE}/external/broker/public/deposits/pdf/{deposit_id}"
    r = session.get(url)
    r.raise_for_status()
    dest.write_bytes(r.content)
    print(f"    PDF saved: {filename} ({len(r.content) // 1024} KB)")
    return dest


def parse_csv(csv_text: str) -> dict:
    df = pd.read_csv(StringIO(csv_text), header=None, skiprows=1)
    codes = {}
    for _, row in df.iterrows():
        key = str(row[0]).strip()
        try:
            codes[key] = float(row[1])
        except (ValueError, TypeError):
            codes[key] = row[1]
    return codes


def compute_kpis(codes: dict) -> dict:
    def get(code):
        return codes.get(code, 0.0)

    omzet        = get("70")
    cogs         = get("60")
    depreciation = get("630")
    ebit         = get("9901")
    net_profit   = get("9904")
    cash         = get("54/58")
    equity       = get("10/15")
    total_assets = get("20/58")
    fin_debt     = get("17") + get("43")
    gross_profit = omzet - cogs
    ebitda       = ebit + depreciation

    def pct(num, denom):
        return round(num / denom * 100, 2) if denom else None

    return {
        "entity":           codes.get("Entity name"),
        "period_end":       codes.get("Accounting period end date"),
        "chiffre_affaires": omzet,
        "marge_brute":      gross_profit,
        "ebitda":           ebitda,
        "ebit":             ebit,
        "resultat_net":     net_profit,
        "taux_marge_brute": pct(gross_profit, omzet),
        "taux_ebitda":      pct(ebitda, omzet),
        "marge_nette":      pct(net_profit, omzet),
        "tresorerie":       cash,
        "dettes_fin":       fin_debt,
        "dette_nette":      fin_debt - cash,
        "fonds_propres":    equity,
        "total_actif":      total_assets,
        "autonomie_fin":    pct(equity, total_assets),
    }


def get_all_kpis(enterprise_number: str) -> list[dict]:
    session = make_session(enterprise_number)
    deposits = get_deposits(session, enterprise_number)

    results = []
    for deposit in deposits:
        deposit_id = deposit["id"]
        year = deposit["periodEndDateYear"]

        print(f"  Processing {year} (id={deposit_id})...")

        # Always attempt PDF download (works for all years including migrated)
        try:
            download_pdf(session, deposit)
        except Exception as e:
            print(f"    ✗ PDF failed for {year}: {e}")
        time.sleep(0.3)

        # CSV only available for non-migrated filings
        if deposit.get("migration"):
            print(f"    Skipping CSV for {year} (legacy/migrated filing)")
            continue

        try:
            csv_text = download_csv(session, deposit_id)
            codes = parse_csv(csv_text)
            kpis = compute_kpis(codes)
            kpis["year"] = year
            kpis["reference"] = deposit["reference"]
            results.append(kpis)
        except Exception as e:
            print(f"    ✗ CSV failed for {year}: {e}")
        time.sleep(0.3)

    return results


# --- Run ---
enterprise_number = "0203430576"  # Apple Retail Belgium
kpis = get_all_kpis(enterprise_number)

df = pd.DataFrame(kpis).set_index("year").sort_index(ascending=False)
pd.set_option("display.float_format", "{:,.2f}".format)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

print("\n=== KPI Summary ===")
print(df[["entity", "period_end", "chiffre_affaires", "ebitda", "resultat_net", "marge_nette", "autonomie_fin"]])