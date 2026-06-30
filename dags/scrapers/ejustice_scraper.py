"""
ejustice_scraper.py
-------------------
Scraping des publications eJustice pour une entreprise belge.

Fonctions exposées :
  - fetch_publications(enterprise_number, lang)  → list[dict]
  - store_publications(enterprise_number, publications, hdfs_client)
"""

import json
import logging
import re
import time
from typing import Optional

from bs4 import BeautifulSoup
from hdfs import InsecureClient

from scrapers.tor_session import make_session

logger = logging.getLogger(__name__)

EJUSTICE_BASE = "https://www.ejustice.just.fgov.be"
EJUSTICE_LIST = f"{EJUSTICE_BASE}/cgi_tsv/list.pl"

HDFS_BASE = "/data/raw"


def _hdfs_client() -> InsecureClient:
    from hdfs import InsecureClient
    return InsecureClient("http://namenode:9870", user="airflow")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Scraping des publications
# ─────────────────────────────────────────────────────────────────────────────

def _parse_publication_item(item) -> dict:
    """Extrait les métadonnées d'un div.list-item eJustice."""
    # Dénomination (sous-titre)
    subtitle = item.select_one("p.list-item--subtitle")
    denom    = subtitle.get_text(" ", strip=True) if subtitle else ""

    # Date / NUMAC / type depuis le lien titre
    title_a = item.select_one("a.list-item--title")
    lines   = []
    if title_a:
        lines = [
            l.strip()
            for l in title_a.get_text("\n", strip=True).split("\n")
            if l.strip()
        ]

    date, numac, type_pub = "", "", ""
    if lines:
        m = re.match(r"(\d{4}-\d{2}-\d{2})\s*/\s*(\d+)", lines[-1])
        if m:
            date   = m.group(1)
            numac  = m.group(2)
            type_pub = lines[-2] if len(lines) >= 2 else ""

    # Lien PDF
    image_a  = item.select_one("a.standard")
    lien_pdf = ""
    if image_a and image_a.get("href"):
        href = image_a["href"]
        lien_pdf = (EJUSTICE_BASE + href) if href.startswith("/") else href

    # Lien détail
    detail_a   = item.select_one("a.read-more")
    lien_detail = ""
    if detail_a and detail_a.get("href"):
        href = detail_a["href"]
        lien_detail = (
            f"{EJUSTICE_BASE}/cgi_tsv/{href}"
            if not href.startswith("http") else href
        )

    return {
        "date":        date,
        "numac":       numac,
        "type":        type_pub,
        "denomination": denom,
        "lien_pdf":    lien_pdf,
        "lien_detail": lien_detail,
    }


def fetch_publications(
    enterprise_number: str,
    lang: str = "fr",
) -> list[dict]:
    """
    Scrape toutes les publications eJustice pour une entreprise.

    Parameters
    ----------
    enterprise_number : str   (format XXXX.XXX.XXX ou XXXXXXXXXX)
    lang              : str   'fr' ou 'nl'

    Returns
    -------
    list[dict] : publications avec date, numac, type, lien_pdf, lien_detail
    """
    btw = enterprise_number.replace(".", "")

    # Session avec headers Firefox complets pour éviter le bot-detection
    session = make_session()
    session.headers.update({
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":   "document",
        "Sec-Fetch-Mode":   "navigate",
        "Sec-Fetch-Site":   "none",
        "Sec-Fetch-User":   "?1",
    })

    # Warm-up : visite de la page d'accueil
    try:
        session.get(EJUSTICE_BASE + "/", timeout=15)
        time.sleep(1)
    except Exception as exc:
        logger.warning(f"[eJustice] Warm-up échoué : {exc}")

    publications = []
    page         = 1

    while True:
        logger.info(f"[eJustice] Page {page} pour {enterprise_number}")

        try:
            resp = session.get(
                EJUSTICE_LIST,
                params={"language": lang, "btw": btw, "page": page},
                timeout=20,
            )
        except Exception as exc:
            logger.error(f"[eJustice] Erreur réseau page {page} : {exc}")
            break

        if resp.status_code != 200:
            logger.error(f"[eJustice] Statut {resp.status_code} page {page}")
            break

        soup  = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("div.list-item")

        if not items:
            logger.info(f"[eJustice] Aucun item page {page} → fin")
            break

        for item in items:
            publications.append(_parse_publication_item(item))

        logger.info(f"[eJustice] Page {page} → {len(items)} publications")

        # eJustice affiche 10 items par page
        if len(items) < 10:
            break

        page += 1
        time.sleep(2)

    logger.info(f"[eJustice] Total : {len(publications)} publications pour {enterprise_number}")
    return publications


# ─────────────────────────────────────────────────────────────────────────────
# 2. Stockage HDFS
# ─────────────────────────────────────────────────────────────────────────────

def store_publications(
    enterprise_number: str,
    publications: list[dict],
    hdfs_client: Optional[InsecureClient] = None,
) -> str:
    """
    Stocke les publications eJustice en JSON dans HDFS.

    Returns
    -------
    str : chemin HDFS du fichier créé
    """
    if hdfs_client is None:
        hdfs_client = _hdfs_client()

    hdfs_dir  = f"{HDFS_BASE}/{enterprise_number}/ejustice"
    hdfs_path = f"{hdfs_dir}/publications.json"

    hdfs_client.makedirs(hdfs_dir)

    payload = json.dumps(publications, ensure_ascii=False, indent=2).encode("utf-8")

    with hdfs_client.write(hdfs_path, overwrite=True) as f:
        f.write(payload)

    logger.info(
        f"[eJustice] {len(publications)} publications → {hdfs_path} "
        f"({len(payload) // 1024} Ko)"
    )
    return hdfs_path