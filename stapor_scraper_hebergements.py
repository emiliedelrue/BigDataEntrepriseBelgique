"""
stapor_scraper_hebergements.py
------------------------------
Téléchargement des statuts notariaux vers HDFS Silver.
Lecture des entreprises depuis la collection MongoDB 'hebergements'.
Suivi de l'avancement dans 'state_db_hebergements'.

"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from playwright.sync_api import sync_playwright
from pymongo import MongoClient

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BASE        = "https://statuts.notaire.be/stapor_v1"
COOKIE_FILE = Path("notaire_cookies.json")
PAGE_SIZE   = 20
HDFS_SILVER = "/data/silver"
MONGO_URL   = "mongodb://localhost:27017"
MONGO_DB    = "belgique"

# Tor — 3 conteneurs Docker avec round-robin
USE_TOR      = True
TOR_PASSWORD = "mypass"
TOR_NODES = [
    {"socks": "socks5h://127.0.0.1:9050", "control_port": 9051},  # tor1
    {"socks": "socks5h://127.0.0.1:9052", "control_port": 9053},  # tor2
    {"socks": "socks5h://127.0.0.1:9054", "control_port": 9055},  # tor3
]
_tor_idx = 0  # index courant (round-robin)

HDFS_CONTAINER = "hdfs-namenode"

HEADERS_API = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-origin",
}


# ── MongoDB ───────────────────────────────────────────────────────────────────

_mongo_client: MongoClient | None = None

def _get_db():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(MONGO_URL)
    return _mongo_client[MONGO_DB]


# ── Tor ───────────────────────────────────────────────────────────────────────

def _next_tor_proxy() -> str:
    """Passe au prochain nœud Tor en round-robin, retourne son URL SOCKS5."""
    global _tor_idx
    _tor_idx = (_tor_idx + 1) % len(TOR_NODES)
    proxy = TOR_NODES[_tor_idx]["socks"]
    log.info(f"  ↻ Tor : nœud {_tor_idx + 1}/3 ({proxy})")
    return proxy


def _rotate_tor_circuit(node_idx: int | None = None) -> bool:
    """Envoie NEWNYM au nœud Tor pour obtenir une nouvelle IP de sortie."""
    idx = node_idx if node_idx is not None else _tor_idx
    ctrl_port = TOR_NODES[idx]["control_port"]
    try:
        from stem import Signal
        from stem.control import Controller
        with Controller.from_port(port=ctrl_port) as ctrl:
            ctrl.authenticate(password=TOR_PASSWORD)
            ctrl.signal(Signal.NEWNYM)
        time.sleep(2)
        log.info(f"  ↻ Circuit Tor nœud {idx + 1} renouvelé")
        return True
    except Exception as e:
        log.warning(f"  Tor rotate nœud {idx + 1} impossible : {e}")
        return False


# ── Session / Cookies ─────────────────────────────────────────────────────────

def _fetch_cookies_via_playwright(seed_bce: str, tor_proxy: str | None = None) -> list:
    """
    Lance Playwright dans un thread séparé (compatibilité Jupyter asyncio).
    Si tor_proxy est fourni (ex: 'socks5h://127.0.0.1:9050'), Chrome passe par Tor
    pour que les cookies et les requêtes API viennent de la même IP.
    """
    seed_url = (
        f"{BASE}/enterprise/{seed_bce}/statutes"
        f"?enterpriseNumber={seed_bce}&statuteStart=0&statuteCount=5"
    )
    proxy_info = f" via {tor_proxy}" if tor_proxy else ""
    log.info(f"Ouverture Chrome pour renouveler les cookies{proxy_info}...")

    result: list = []
    error:  list = []

    def _run() -> None:
        with sync_playwright() as p:
            # Chromium utilise socks5:// (sans le h) comme argument de lancement
            launch_args = []
            if tor_proxy:
                chromium_proxy = tor_proxy.replace("socks5h://", "socks5://")
                launch_args.append(f"--proxy-server={chromium_proxy}")

            try:
                browser = p.chromium.launch(channel="chrome", headless=False, args=launch_args)
            except Exception:
                browser = p.chromium.launch(headless=False, args=launch_args)

            ctx  = browser.new_context(locale="fr-BE", user_agent=HEADERS_API["User-Agent"])
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

            result.extend(ctx.cookies())
            browser.close()

    t = threading.Thread(target=_run)
    t.start()
    t.join()

    if error:
        raise error[0]
    return result


def _build_session(cookies: list, tor_proxy: str | None = None) -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS_API)
    proxy = tor_proxy or (TOR_NODES[_tor_idx]["socks"] if USE_TOR else None)
    if proxy:
        session.proxies = {"http": proxy, "https": proxy}
        log.info(f"  Session via Tor ({proxy})")
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c["domain"])
    return session


def _session_valid(session: requests.Session, seed_bce: str) -> bool:
    try:
        r = session.get(
            f"{BASE}/api/enterprises/{seed_bce}/statutes",
            params={"offset": 0, "limit": 1},
            timeout=10,
        )
        return "application/json" in r.headers.get("content-type", "")
    except Exception:
        return False


def get_session(seed_bce: str = "0836157420") -> requests.Session:
    """
    Retourne une session valide.
    Si USE_TOR, les cookies sont obtenus via Chrome à travers Tor
    (même IP que les requêtes API → pas de 403).
    """
    tor_proxy = TOR_NODES[_tor_idx]["socks"] if USE_TOR else None

    if COOKIE_FILE.exists():
        cookies = json.loads(COOKIE_FILE.read_text())
        session = _build_session(cookies, tor_proxy=tor_proxy)
        if _session_valid(session, seed_bce):
            log.info("Session OK (cookies en cache)")
            return session
        log.info("Cookies expirés — renouvellement...")

    cookies = _fetch_cookies_via_playwright(seed_bce, tor_proxy=tor_proxy)
    COOKIE_FILE.write_text(json.dumps(cookies, indent=2))
    return _build_session(cookies, tor_proxy=tor_proxy)


# ── Fetch statuts ─────────────────────────────────────────────────────────────

def get_statutes(session: requests.Session, enterprise_number: str, seed_bce: str = "") -> list:
    """
    Récupère tous les statuts DONE pour une entreprise.
    Gère 429, 403 (IP bannie) et session expirée avec renouvellement automatique.
    """
    num = enterprise_number.replace(".", "")
    url = f"{BASE}/api/enterprises/{num}/statutes"
    session.headers["Referer"] = (
        f"{BASE}/enterprise/{num}/statutes"
        f"?enterpriseNumber={num}&statuteStart=0&statuteCount=5"
    )

    all_statutes, offset = [], 0
    retries = 0

    while True:
        r = session.get(
            url,
            params={"deedDate": "", "offset": offset, "limit": PAGE_SIZE},
            timeout=15,
        )

        # 429 — rate limit : changer de nœud Tor ou attendre
        if r.status_code == 429:
            if retries >= len(TOR_NODES):
                log.error(f"  [{num}] 429 sur tous les nœuds Tor — entreprise ignorée")
                break
            if USE_TOR:
                log.warning(f"  [{num}] 429 — passage nœud Tor suivant")
                new_proxy = _next_tor_proxy()
                session.proxies = {"http": new_proxy, "https": new_proxy}
            else:
                log.warning(f"  [{num}] 429 — attente 30s")
                time.sleep(30)
            retries += 1
            continue

        # 403 — IP Tor bannie : changer de nœud + refetch cookies via le NOUVEAU nœud
        if r.status_code == 403:
            if retries >= len(TOR_NODES):
                log.error(f"  [{num}] 403 sur tous les nœuds Tor — entreprise ignorée")
                break
            log.warning(f"  [{num}] 403 Forbidden — changement nœud Tor + renouvellement cookies")
            new_proxy = _next_tor_proxy() if USE_TOR else None
            if new_proxy:
                session.proxies = {"http": new_proxy, "https": new_proxy}
            seed = seed_bce or num
            # Playwright passe par le NOUVEAU nœud Tor → cookies liés à la nouvelle IP
            new_cookies = _fetch_cookies_via_playwright(seed, tor_proxy=new_proxy)
            COOKIE_FILE.write_text(json.dumps(new_cookies, indent=2))
            for c in new_cookies:
                session.cookies.set(c["name"], c["value"], domain=c["domain"])
            retries += 1
            time.sleep(2)
            continue

        r.raise_for_status()

        # Session expirée (réponse HTML au lieu de JSON)
        if "application/json" not in r.headers.get("content-type", ""):
            if retries >= 2:
                log.error(f"  [{num}] Session toujours invalide après renouvellement — abandon")
                break
            log.warning(f"  [{num}] Session expirée — renouvellement cookies...")
            seed = seed_bce or num
            cur_proxy = TOR_NODES[_tor_idx]["socks"] if USE_TOR else None
            new_cookies = _fetch_cookies_via_playwright(seed, tor_proxy=cur_proxy)
            COOKIE_FILE.write_text(json.dumps(new_cookies, indent=2))
            for c in new_cookies:
                session.cookies.set(c["name"], c["value"], domain=c["domain"])
            retries += 1
            time.sleep(2)
            continue

        retries = 0  # reset après succès
        data  = r.json()
        batch = data.get("statutes", [])
        all_statutes.extend(batch)
        log.info(f"  [{num}] offset={offset} — {len(batch)} statuts (total: {data.get('totalItems', 0)})")

        if not batch or len(all_statutes) >= data.get("totalItems", 0):
            break
        offset += PAGE_SIZE
        time.sleep(1)

    done = [s for s in all_statutes if s.get("documentStatus") == "DONE"]
    log.info(f"  [{num}] → {len(done)} statuts DONE")
    return done


# ── HDFS ──────────────────────────────────────────────────────────────────────

def _hdfs_exists(hdfs_path: str) -> bool:
    result = subprocess.run(
        ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-test", "-e", hdfs_path],
        capture_output=True,
    )
    return result.returncode == 0


def _hdfs_write(content: bytes, hdfs_path: str) -> None:
    """Écrit des bytes dans HDFS via docker cp + hdfs dfs -put."""
    hdfs_dir = str(Path(hdfs_path).parent)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(content)
        tmp_local = tmp.name

    container_tmp = f"/tmp/{os.path.basename(tmp_local)}"
    try:
        subprocess.run(
            ["docker", "cp", tmp_local, f"{HDFS_CONTAINER}:{container_tmp}"],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-put", "-f", container_tmp, hdfs_path],
            check=True, capture_output=True,
        )
    finally:
        os.unlink(tmp_local)
        subprocess.run(
            ["docker", "exec", HDFS_CONTAINER, "rm", "-f", container_tmp],
            capture_output=True,
        )


# ── Téléchargement PDF ────────────────────────────────────────────────────────

def download_statute_pdf(
    session: requests.Session,
    enterprise_number: str,
    statute: dict,
) -> Optional[str]:
    """Télécharge un PDF et le stocke dans HDFS. Retourne le chemin HDFS ou None."""
    num_api   = enterprise_number.replace(".", "")
    doc_id    = statute["documentId"]
    deed_date = statute.get("deedDate", "unknown").replace("-", "")
    hdfs_path = f"{HDFS_SILVER}/{enterprise_number}/stapor/{deed_date}_{doc_id}.pdf"

    if _hdfs_exists(hdfs_path):
        log.info(f"    Déjà dans HDFS : {hdfs_path}")
        return hdfs_path

    r = session.get(
        f"{BASE}/api/enterprises/{num_api}/statutes/non-certified/{doc_id}",
        timeout=30,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    if "pdf" not in r.headers.get("content-type", "") and len(r.content) < 1000:
        return None

    _hdfs_write(r.content, hdfs_path)
    log.info(f"    → HDFS : {hdfs_path} ({len(r.content) // 1024} KB)")
    return hdfs_path


# ── State DB ──────────────────────────────────────────────────────────────────

def _mark_done(enterprise_number: str, downloaded: list) -> None:
    """Enregistre le résultat dans state_db_hebergements."""
    db = _get_db()
    ok_paths = [s["hdfs_path"] for s in downloaded if s.get("hdfs_path")]
    db.state_db_hebergements.update_one(
        {"enterprise_number": enterprise_number},
        {"$set": {
            "enterprise_number":    enterprise_number,
            "stapor_status":        "done",
            "stapor_pdfs_count":    len(ok_paths),
            "stapor_hdfs_paths":    ok_paths,
            "stapor_processed_at":  datetime.now(timezone.utc),
        }},
        upsert=True,
    )


def _already_done(enterprise_numbers: list) -> set:
    """Retourne les numéros déjà traités dans state_db_hebergements."""
    db = _get_db()
    return {
        doc["enterprise_number"]
        for doc in db.state_db_hebergements.find(
            {
                "enterprise_number": {"$in": enterprise_numbers},
                "stapor_status": "done",
            },
            {"enterprise_number": 1},
        )
    }


# ── Pipeline principal ────────────────────────────────────────────────────────

def run(enterprise_numbers: list, seed_bce: Optional[str] = None) -> dict:
    """
    Scrape les statuts notariaux pour une liste de numéros BCE.

    Parameters
    ----------
    enterprise_numbers : liste de numéros BCE (format XXXX.XXX.XXX)
    seed_bce           : numéro pour le warm-up Playwright (défaut = premier)

    Returns
    -------
    dict {enterprise_number: [statute_dict, ...]}
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    seed = (seed_bce or enterprise_numbers[0]).replace(".", "")
    session = get_session(seed_bce=seed)

    # Filtrer les entreprises déjà traitées
    done_set = _already_done(enterprise_numbers)
    if done_set:
        log.info(f"  {len(done_set)} entreprises déjà dans state_db — skip")
    todo = [n for n in enterprise_numbers if n not in done_set]
    log.info(f"  {len(todo)} entreprises à traiter")

    results = {}
    for i, num in enumerate(todo):
        log.info(f"\n{'='*55}\n  Entreprise {i+1}/{len(todo)} : {num}\n{'='*55}")

        # Round-robin Tor : changer de nœud à chaque entreprise
        if USE_TOR and i > 0:
            new_proxy = _next_tor_proxy()
            session.proxies = {"http": new_proxy, "https": new_proxy}

        statutes = get_statutes(session, num, seed_bce=seed)
        downloaded = []

        for statute in statutes:
            hdfs_path = download_statute_pdf(session, num, statute)
            downloaded.append({**statute, "hdfs_path": hdfs_path})
            time.sleep(0.5)

        results[num] = downloaded

        ok  = sum(1 for s in downloaded if s.get("hdfs_path"))
        nok = len(downloaded) - ok
        log.info(f"  Résultat : {ok} PDFs stockés, {nok} ignorés")

        # Sauvegarde dans state_db_hebergements
        _mark_done(num, downloaded)

    return results


# ── Point d'entrée hôtellerie ─────────────────────────────────────────────────

def run_hebergements(
    skip: int = 0,
    limit: int = 0,
    seed_bce: Optional[str] = None,
) -> dict:
    """
    Lit les entreprises depuis MongoDB 'hebergements' et scrape leurs statuts.

    Parameters
    ----------
    skip     : offset MongoDB (reprendre depuis un certain point)
    limit    : nombre max d'entreprises (0 = toutes)
    seed_bce : numéro BCE pour le warm-up Playwright

    Usage :
        ss.run_hebergements()                    # toutes les entreprises
        ss.run_hebergements(skip=500, limit=500) # batch 2
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    db = _get_db()

    query  = {"status": "AC"}
    cursor = db.hebergements.find(query, {"enterprise_number": 1}, skip=skip)
    if limit:
        cursor = cursor.limit(limit)

    enterprise_numbers = [doc["enterprise_number"] for doc in cursor]
    total = db.hebergements.count_documents(query)

    log.info(
        f"[Stapor] {len(enterprise_numbers)} hôtels chargés depuis MongoDB "
        f"(skip={skip}, total={total:,})"
    )

    if not enterprise_numbers:
        log.warning("[Stapor] Aucune entreprise trouvée dans 'hebergements'")
        return {}

    return run(enterprise_numbers, seed_bce=seed_bce)