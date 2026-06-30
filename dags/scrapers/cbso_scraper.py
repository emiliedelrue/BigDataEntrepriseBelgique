"""
cbso_scraper.py
---------------
Récupération des comptes annuels depuis consult.cbso.nbb.be.

Fonctions exposées :
  - fetch_deposit_list(enterprise_number)  → list[dict]
  - filter_deposits(deposits)              → dict[str, dict]  (annee → depot)
  - download_pdfs(enterprise_number, par_annee, hdfs_client, start_year)
  - download_csvs(enterprise_number, par_annee, hdfs_client, start_year)
"""

import logging
import time
from typing import Optional

from hdfs import InsecureClient

from scrapers.tor_session import get_with_rotation

logger = logging.getLogger(__name__)

# ── Constantes CBSO ───────────────────────────────────────────────────────────
CBSO_API      = "https://consult.cbso.nbb.be/api/rs-consult/published-deposits"
CBSO_DOC_BASE = "https://consult.cbso.nbb.be/api/external/broker/public/deposits"
CBSO_HOME     = "https://consult.cbso.nbb.be/consult-enterprise"

# ── HDFS ─────────────────────────────────────────────────────────────────────
HDFS_URL      = "http://namenode:9870"   # nom de service Docker
HDFS_BASE     = "/data/raw"


def _hdfs_client() -> InsecureClient:
    return InsecureClient(HDFS_URL, user="airflow")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Récupération de la liste des dépôts
# ─────────────────────────────────────────────────────────────────────────────

def fetch_deposit_list(enterprise_number: str) -> list[dict]:
    """
    Récupère tous les dépôts CBSO pour une entreprise.

    Parameters
    ----------
    enterprise_number : str
        Numéro BCE au format XXXX.XXX.XXX ou XXXXXXXXXX

    Returns
    -------
    list[dict] : liste brute des dépôts retournés par l'API CBSO
    """
    num_clean = enterprise_number.replace(".", "")
    init_url  = f"{CBSO_HOME}/{num_clean}"

    deposits = []
    page     = 0

    while True:
        params = {
            "enterpriseNumber": num_clean,
            "page": page,
            "size": 50,
            "sort": ["periodEndDate,desc", "depositDate,desc"],
        }

        logger.info(f"[CBSO] Fetch dépôts page={page} pour {enterprise_number}")

        resp = get_with_rotation(
            url=CBSO_API,
            params=params,
            extra_headers={"Referer": init_url},
            init_session_url=init_url,
        )

        if resp.status_code != 200:
            logger.error(f"[CBSO] API retourne {resp.status_code}")
            break

        if not resp.content:
            break

        data = resp.json()
        batch = data.get("content", [])
        deposits.extend(batch)

        logger.info(f"[CBSO] Page {page} → {len(batch)} dépôts | last={data.get('last', True)}")

        if not batch or data.get("last", True):
            break

        page += 1
        time.sleep(1.5)  # politesse entre pages

    logger.info(f"[CBSO] Total : {len(deposits)} dépôts pour {enterprise_number}")
    return deposits


# ─────────────────────────────────────────────────────────────────────────────
# 2. Filtrage : 1 dépôt par année (préférence FR, hors consolidés)
# ─────────────────────────────────────────────────────────────────────────────

def filter_deposits(deposits: list[dict]) -> dict[str, dict]:
    """
    Filtre les dépôts consolidés et conserve le meilleur dépôt par année.

    Priorité : langue FR > langue NL > premier disponible
    """
    # Exclure les comptes consolidés
    deposits = [
        d for d in deposits
        if "consolidé" not in d.get("modelName", "").lower()
        and "geconsolideerde" not in d.get("modelName", "").lower()
    ]

    par_annee: dict[str, dict] = {}

    for d in deposits:
        annee = str(d.get("periodEndDateYear", "?"))
        if annee == "?":
            continue

        if annee not in par_annee:
            par_annee[annee] = d
        else:
            # Préférer FR
            existing_lang = par_annee[annee].get("language", "")
            new_lang      = d.get("language", "")
            if new_lang == "FR" and existing_lang != "FR":
                par_annee[annee] = d

    logger.info(f"[CBSO] Filtrage : {len(par_annee)} années uniques")
    return par_annee


# ─────────────────────────────────────────────────────────────────────────────
# 3. Téléchargement des PDFs → HDFS
# ─────────────────────────────────────────────────────────────────────────────

def download_pdfs(
    enterprise_number: str,
    par_annee: dict[str, dict],
    hdfs_client: Optional[InsecureClient] = None,
    start_year: int = 2000,
) -> dict[str, str]:
    """
    Télécharge les PDF annuels et les stocke dans HDFS.

    Returns
    -------
    dict[str, str] : {annee: hdfs_path} pour les PDFs téléchargés avec succès
    """
    if hdfs_client is None:
        hdfs_client = _hdfs_client()

    hdfs_dir  = f"{HDFS_BASE}/{enterprise_number}/cbso/pdfs"
    hdfs_client.makedirs(hdfs_dir)

    results: dict[str, str] = {}

    for annee, depot in sorted(par_annee.items()):
        an = int(annee) if annee.isdigit() else 0
        if an < start_year:
            continue

        did       = depot["id"]
        pdf_url   = f"{CBSO_DOC_BASE}/pdf/{did}"
        hdfs_path = f"{hdfs_dir}/{annee}.pdf"

        # Skip si déjà présent
        try:
            hdfs_client.status(hdfs_path)
            logger.info(f"[CBSO PDF] {annee} déjà dans HDFS — skip")
            results[annee] = hdfs_path
            continue
        except Exception:
            pass

        logger.info(f"[CBSO PDF] Téléchargement {annee} ({did[:8]}...)")
        time.sleep(1)

        try:
            resp = get_with_rotation(pdf_url, timeout=60)

            if resp.status_code == 200 and len(resp.content) > 1000:
                with hdfs_client.write(hdfs_path, overwrite=True) as f:
                    f.write(resp.content)
                size_kb = len(resp.content) // 1024
                logger.info(f"[CBSO PDF] {annee} → {hdfs_path} ({size_kb} Ko)")
                results[annee] = hdfs_path
            else:
                logger.warning(f"[CBSO PDF] {annee} : statut {resp.status_code} ou fichier vide")

        except Exception as exc:
            logger.error(f"[CBSO PDF] {annee} : erreur {exc}")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 4. Téléchargement des CSVs → HDFS
# ─────────────────────────────────────────────────────────────────────────────

def download_csvs(
    enterprise_number: str,
    par_annee: dict[str, dict],
    hdfs_client: Optional[InsecureClient] = None,
    start_year: int = 2021,
) -> dict[str, str]:
    """
    Télécharge les CSV annuels (endpoint /consult/csv/) et les stocke dans HDFS.

    Returns
    -------
    dict[str, str] : {annee: hdfs_path} pour les CSVs téléchargés avec succès
    """
    if hdfs_client is None:
        hdfs_client = _hdfs_client()

    hdfs_dir  = f"{HDFS_BASE}/{enterprise_number}/cbso/csvs"
    hdfs_client.makedirs(hdfs_dir)

    results: dict[str, str] = {}

    for annee, depot in sorted(par_annee.items()):
        an = int(annee) if annee.isdigit() else 0
        if an < start_year:
            continue

        did       = depot["id"]
        csv_url   = f"{CBSO_DOC_BASE}/consult/csv/{did}"
        hdfs_path = f"{hdfs_dir}/{annee}.csv"

        # Skip si déjà présent
        try:
            hdfs_client.status(hdfs_path)
            logger.info(f"[CBSO CSV] {annee} déjà dans HDFS — skip")
            results[annee] = hdfs_path
            continue
        except Exception:
            pass

        logger.info(f"[CBSO CSV] Téléchargement {annee} ({did[:8]}...)")
        time.sleep(1)

        try:
            resp = get_with_rotation(
                csv_url,
                extra_headers={"Accept": "text/csv,application/octet-stream,*/*"},
                timeout=30,
            )

            content_type = resp.headers.get("Content-Type", "")

            if resp.status_code == 200 and "problem+json" not in content_type:
                with hdfs_client.write(hdfs_path, overwrite=True) as f:
                    f.write(resp.content)
                size_kb = len(resp.content) // 1024
                logger.info(f"[CBSO CSV] {annee} → {hdfs_path} ({size_kb} Ko)")
                results[annee] = hdfs_path
            else:
                logger.warning(
                    f"[CBSO CSV] {annee} : statut {resp.status_code} "
                    f"| Content-Type: {content_type}"
                )

        except Exception as exc:
            logger.error(f"[CBSO CSV] {annee} : erreur {exc}")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 5. Résumé des dépôts (pour les logs Airflow)
# ─────────────────────────────────────────────────────────────────────────────

def summarize_deposits(par_annee: dict[str, dict]) -> list[dict]:
    """Retourne une version sérialisable de par_annee pour XCom Airflow."""
    return [
        {
            "annee":     annee,
            "id":        d["id"],
            "language":  d.get("language"),
            "modelName": d.get("modelName", "")[:60],
            "fileType":  d.get("importFileType"),
        }
        for annee, d in sorted(par_annee.items())
    ]