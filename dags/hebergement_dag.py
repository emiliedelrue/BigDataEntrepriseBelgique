"""
hebergement_dag.py
------------------
DAG Airflow — Couche Silver : hébergements belges.

Flux :
  MongoDB (hebergements) → delta detection (state_db_hebergement) → HDFS Silver

Sources :
  - CBSO/NBB  : PDFs + CSVs des comptes annuels
  - eJustice  : publications (JSON)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

HDFS_URL    = "http://namenode:9870"
HDFS_USER   = "airflow"
HDFS_SILVER = "/data/silver"
STATE_COL   = "state_db_hebergement"


@dag(
    dag_id="hebergement_ingestion_silver",
    description="Ingestion Silver — CBSO / eJustice → HDFS",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=10,
    default_args={
        "retries":                   2,
        "retry_delay":               timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["ingestion", "silver", "hebergement", "layer-2"],
    params={
        "enterprise_number": Param(
            default="",
            type="string",
            description="Numéro BCE ciblé (vide = batch depuis hebergements)",
        ),
        "start_year": Param(default=2020, type="integer"),
        "sources": Param(
            default=["cbso", "ejustice"],
            type="array",
        ),
        "batch_size": Param(default=500, type="integer"),
        "mongo_skip": Param(
            default=0,
            type="integer",
            description="Offset MongoDB pour les runs parallèles",
        ),
    },
)
def hebergement_ingestion_silver():

    @task(task_id="resolve_hebergements")
    def resolve_hebergements(**context) -> list[str]:
        from db.mongo_client import get_db

        params = context["params"]
        num    = params.get("enterprise_number", "").strip()

        if num:
            log.info(f"Mode ciblé : {num}")
            return [num]

        db    = get_db()
        batch = params["batch_size"]
        skip  = params.get("mongo_skip", 0)
        nums  = [
            doc["enterprise_number"]
            for doc in db.hebergements.find(
                {"status": "AC"},
                {"enterprise_number": 1},
                skip=skip,
                limit=batch,
            )
        ]
        log.info(f"Mode bulk : {len(nums)} hébergements depuis MongoDB (skip={skip})")
        return nums

    @task(task_id="ingest_cbso_silver")
    def ingest_cbso_silver(enterprise_numbers: list[str], **context) -> dict:
        import time
        from hdfs import InsecureClient
        from db.state_db import is_done, mark_done, mark_error
        from scrapers.cbso_scraper import fetch_deposit_list, filter_deposits, CBSO_DOC_BASE
        from scrapers.tor_session import get_with_rotation

        if "cbso" not in context["params"]["sources"]:
            log.info("[CBSO] Source désactivée — skip")
            return {}

        hdfs       = InsecureClient(HDFS_URL, user=HDFS_USER)
        start_year = context["params"]["start_year"]
        results    = {"done": 0, "error": 0, "skipped": 0}

        for num in enterprise_numbers:
            log.info(f"\n[CBSO-Silver] {num}")
            try:
                deposits  = fetch_deposit_list(num)
                par_annee = filter_deposits(deposits)
            except Exception as exc:
                log.error(f"[CBSO-Silver] {num} : fetch échoué — {exc}")
                continue

            for annee, depot in sorted(par_annee.items()):
                an  = int(annee) if annee.isdigit() else 0
                did = depot["id"]

                for file_type, url_fn in [
                    ("pdf", lambda d: f"{CBSO_DOC_BASE}/pdf/{d}"),
                    ("csv", lambda d: f"{CBSO_DOC_BASE}/consult/csv/{d}"),
                ]:
                    if an < start_year:
                        continue

                    if is_done(num, "cbso", did, file_type, collection_name=STATE_COL):
                        results["skipped"] += 1
                        continue

                    hdfs_path = f"{HDFS_SILVER}/{num}/cbso/{file_type}s/{annee}.{file_type}"
                    time.sleep(1)

                    try:
                        resp = get_with_rotation(url_fn(did), timeout=60)

                        if resp.status_code == 200 and len(resp.content) > 500:
                            hdfs.makedirs(f"{HDFS_SILVER}/{num}/cbso/{file_type}s")
                            with hdfs.write(hdfs_path, overwrite=True) as f:
                                f.write(resp.content)
                            mark_done(num, "cbso", did, file_type, hdfs_path,
                                      len(resp.content), collection_name=STATE_COL)
                            results["done"] += 1
                        else:
                            mark_error(num, "cbso", did, file_type,
                                       f"HTTP {resp.status_code} / taille {len(resp.content)}",
                                       collection_name=STATE_COL)
                            results["error"] += 1

                    except Exception as exc:
                        mark_error(num, "cbso", did, file_type, str(exc),
                                   collection_name=STATE_COL)
                        results["error"] += 1

            time.sleep(1)

        log.info(f"[CBSO-Silver] Résultat : {results}")
        return results

    @task(task_id="ingest_ejustice_silver")
    def ingest_ejustice_silver(enterprise_numbers: list[str], **context) -> dict:
        import json, time
        from hdfs import InsecureClient
        from db.state_db import is_done, mark_done, mark_error
        from scrapers.ejustice_scraper import fetch_publications

        if "ejustice" not in context["params"]["sources"]:
            log.info("[eJustice] Source désactivée — skip")
            return {}

        hdfs    = InsecureClient(HDFS_URL, user=HDFS_USER)
        results = {"done": 0, "skipped": 0, "error": 0}

        for num in enterprise_numbers:
            log.info(f"\n[eJustice-Silver] {num}")
            deposit_id = f"ejustice_{num}"

            if is_done(num, "ejustice", deposit_id, "json", collection_name=STATE_COL):
                results["skipped"] += 1
                continue

            try:
                pubs      = fetch_publications(num, lang="fr")
                payload   = json.dumps(pubs, ensure_ascii=False, indent=2).encode()
                hdfs_path = f"{HDFS_SILVER}/{num}/ejustice/publications.json"

                hdfs.makedirs(f"{HDFS_SILVER}/{num}/ejustice")
                with hdfs.write(hdfs_path, overwrite=True) as f:
                    f.write(payload)

                mark_done(num, "ejustice", deposit_id, "json", hdfs_path,
                          len(payload), collection_name=STATE_COL)
                results["done"] += 1

            except Exception as exc:
                mark_error(num, "ejustice", deposit_id, "json", str(exc),
                           collection_name=STATE_COL)
                results["error"] += 1

            time.sleep(1)

        log.info(f"[eJustice-Silver] Résultat : {results}")
        return results

    @task(task_id="ingestion_report_silver")
    def ingestion_report_silver(
        enterprise_numbers: list[str],
        cbso_result: dict,
        ejustice_result: dict,
    ) -> dict:
        from db.state_db import get_stats

        log.info("\n" + "=" * 60)
        log.info("RAPPORT INGESTION SILVER — Hébergements")
        log.info(f"  Entreprises traitées : {len(enterprise_numbers)}")
        log.info(f"  CBSO     : {cbso_result}")
        log.info(f"  eJustice : {ejustice_result}")

        if enterprise_numbers:
            stats = get_stats(enterprise_numbers[0], collection_name=STATE_COL)
            log.info(f"\n  State DB ({enterprise_numbers[0]}) : {stats}")

        log.info("=" * 60)

        return {
            "enterprises": len(enterprise_numbers),
            "cbso":        cbso_result,
            "ejustice":    ejustice_result,
        }

    nums  = resolve_hebergements()
    cbso  = ingest_cbso_silver(nums)
    ej    = ingest_ejustice_silver(nums)
    ingestion_report_silver(nums, cbso, ej)


hebergement_ingestion_silver()