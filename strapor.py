import json
import logging
import time
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

log = logging.getLogger(__name__)

BASE        = "https://statuts.notaire.be/stapor_v1"
COOKIE_FILE = Path("notaire_cookies.json")
TMP_PDFS    = Path("tmp/notaire")
TMP_PDFS.mkdir(parents=True, exist_ok=True)
PAGE_SIZE   = 20

HEADERS_API = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-origin",
}

NO_NOTAIRE_FORMS = {"009", "017", "018", "025", "026", "027", "051", "052"}
SEED_BCE = "0836157420"


def _fetch_cookies_via_playwright() -> list[dict]:
    """
    Ouvre ton Chrome installé (visible ~3s) pour passer le challenge F5.
    Retourne la liste brute de cookies Playwright.
    """
    seed_url = (
        f"{BASE}/enterprise/{SEED_BCE}/statutes"
        f"?enterpriseNumber={SEED_BCE}&statuteStart=0&statuteCount=5"
    )
    log.info("Ouverture Chrome pour renouveler les cookies F5...")

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=False)
        except Exception:
            log.warning("Chrome introuvable — fallback sur Chromium")
            browser = p.chromium.launch(headless=False)

        ctx  = browser.new_context(
            locale="fr-BE",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )


        page.goto("https://statuts.notaire.be/", wait_until="load", timeout=20_000)
        page.wait_for_timeout(2000)

        page.goto(seed_url, wait_until="load", timeout=30_000)

        for i in range(40):
            names = {c["name"] for c in ctx.cookies()}
            if "OClmoOot" in names and "Lyp1CWKh" in names:
                log.info(f"  Cookies OK ({i * 500}ms)")
                break
            page.wait_for_timeout(500)
        else:
            log.warning(f"  Timeout — cookies présents : {[c['name'] for c in ctx.cookies()]}")

        cookies = ctx.cookies()
        browser.close()

    return cookies


def _build_session(cookies: list[dict]) -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS_API)
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c["domain"])
    return session


def _session_valid(session: requests.Session) -> bool:
    """Ping rapide — True si on reçoit du JSON (session encore valide)."""
    try:
        r = session.get(
            f"{BASE}/api/enterprises/{SEED_BCE}/statutes",
            params={"offset": 0, "limit": 1},
            timeout=10,
        )
        return "application/json" in r.headers.get("content-type", "")
    except Exception:
        return False


def get_session() -> requests.Session:
    """
    Retourne une session requests valide.
    - Charge notaire_cookies.json si disponible et encore valides
    - Relance Playwright automatiquement sinon (Chrome s'ouvre ~3s)
    """
    if COOKIE_FILE.exists():
        cookies = json.loads(COOKIE_FILE.read_text())
        session = _build_session(cookies)
        if _session_valid(session):
            log.info("Session OK (cookies en cache)")
            return session
        log.info("Cookies expirés — renouvellement automatique...")

    cookies = _fetch_cookies_via_playwright()
    COOKIE_FILE.write_text(json.dumps(cookies, indent=2))
    log.info(f"Cookies sauvegardés → {COOKIE_FILE}")
    return _build_session(cookies)


def get_statutes(session: requests.Session, enterprise_number: str) -> list[dict]:
    url = f"{BASE}/api/enterprises/{enterprise_number}/statutes"
    session.headers["Referer"] = (
        f"{BASE}/enterprise/{enterprise_number}/statutes"
        f"?enterpriseNumber={enterprise_number}&statuteStart=0&statuteCount=5"
    )
    all_statutes, offset = [], 0

    while True:
        r = session.get(url, params={"deedDate": "", "offset": offset, "limit": PAGE_SIZE}, timeout=15)
        r.raise_for_status()

        if "application/json" not in r.headers.get("content-type", ""):
            log.error(f"[{enterprise_number}] Réponse non-JSON — session expirée mid-run")
            break

        data  = r.json()
        batch = data.get("statutes", [])
        total = data.get("totalItems", 0)
        all_statutes.extend(batch)
        log.info(f"  [{enterprise_number}] offset={offset} — {len(batch)} statuts (total: {total})")

        if not batch or len(all_statutes) >= total:
            break
        offset += PAGE_SIZE
        time.sleep(0.3)

    done = [s for s in all_statutes if s.get("documentStatus") == "DONE"]
    log.info(f"  [{enterprise_number}] → {len(done)} DONE")
    return done


def download_statute_pdf(
    session: requests.Session,
    enterprise_number: str,
    statute: dict,
    dest_dir: Path = TMP_PDFS,
) -> Path | None:
    doc_id    = statute["documentId"]
    deed_date = statute.get("deedDate", "unknown").replace("-", "")
    dest      = dest_dir / f"{enterprise_number}_{deed_date}_{doc_id}.pdf"

    if dest.exists():
        log.info(f"    Déjà présent : {dest.name}")
        return dest

    r = session.get(
        f"{BASE}/api/enterprises/{enterprise_number}/statutes/non-certified/{doc_id}",
        timeout=30,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    if "pdf" not in r.headers.get("content-type", "") and len(r.content) < 1000:
        return None

    dest.write_bytes(r.content)
    log.info(f"    Sauvegardé : {dest.name} ({len(r.content) // 1024} KB)")
    return dest


def get_all_statutes(
    enterprise_number: str,
    session: requests.Session | None = None,
    dest_dir: Path = TMP_PDFS,
) -> list[dict]:
    if session is None:
        session = get_session()
    statutes = get_statutes(session, enterprise_number)
    results  = []
    for statute in statutes:
        pdf_path = download_statute_pdf(session, enterprise_number, statute, dest_dir)
        results.append({**statute, "local_pdf": str(pdf_path) if pdf_path else None})
        time.sleep(0.3)
    return results


def needs_notaire_check(forme_juridique: str, status: str = "Active") -> bool:
    return status == "Active" and forme_juridique not in NO_NOTAIRE_FORMS


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    ENTREPRISES = {
        "Apple Retail Belgium": "0836157420",
        "Google Belgium":       "0878065378",
        "SNCB":                 "0203430576",
    }

    session = get_session()  # Playwright seulement si cookies expirés

    for nom, bce in ENTREPRISES.items():
        log.info(f"\n{'='*50}\n{nom} ({bce})")
        statutes = get_all_statutes(bce, session=session)

        if not statutes:
            log.info("  Aucun statut disponible.")
            continue

        for s in statutes:
            log.info(f"  {'✓' if s['local_pdf'] else '✗'} {s.get('deedDate')}  {s.get('documentTitle')}")