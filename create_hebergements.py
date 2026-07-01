"""
create_hebergements.py
----------------------
Script one-shot : crée la collection MongoDB `hebergements`
en filtrant les entreprises avec au moins une activité NACE 55.x.

Utilise une boucle batch (pas de $merge) pour éviter les timeouts
sur les grandes collections.

Usage :
    python3 create_hebergements.py
"""

import logging
import os
import sys
from datetime import datetime, timezone

from pymongo import UpdateOne

# Permet de lancer depuis la racine du projet (hors contexte Airflow)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "dags"))

from db.mongo_client import get_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BATCH_SIZE = 500

# 9 codes NACE hôtellerie retenus (jour2_instructions.docx)
NACE_CODES_HOTELLERIE = [
    "55100", "55201", "55202", "55203", "55204",
    "55209", "55300", "55400", "55900",
]

# Formes juridiques publiques à exclure
JURIDICAL_FORM_EXCLUS = [
    "110", "114", "116", "117",           # entités publiques
    "301", "302", "303",                   # services fédéraux
    "310", "320", "330", "340", "350",     # autorités régionales
    "400", "411", "412", "413", "414",     # communes, CPAS
    "415", "416", "417", "418", "419", "420",
]

FILTRE_NACE = {
    "status":           "AC",             # actifs uniquement
    "TypeOfEnterprise": "2",              # personnes morales privées
    "legal_form":       {"$nin": JURIDICAL_FORM_EXCLUS},
    "activities": {"$elemMatch": {
        "nace_code":      {"$in": NACE_CODES_HOTELLERIE},
        "classification": "MAIN",         # activité principale uniquement
    }},
}


def run() -> None:
    db  = get_db()
    now = datetime.now(timezone.utc)

    # Vider la collection pour repartir propre avec les bons filtres
    db.hebergements.drop()
    log.info("Collection 'hebergements' vidée")

    # Index (créés avant toute écriture)
    db.hebergements.create_index("enterprise_number", unique=True)
    db.hebergements.create_index("status")
    db.hebergements.create_index([("activities.nace_code", 1)])
    log.info("Index créés")

    total_source = db.enterprises.count_documents(FILTRE_NACE)
    log.info(f"Entreprises NACE 55.x dans 'enterprises' : {total_source:,}")

    inserted = 0
    updated  = 0
    errors   = 0
    skip     = 0

    while True:
        # Requête fraîche à chaque lot — pas de curseur long
        batch = list(db.enterprises.find(FILTRE_NACE, skip=skip, limit=BATCH_SIZE))
        if not batch:
            break

        ops = []
        for doc in batch:
            doc["layer"]     = "silver"
            doc["synced_at"] = now
            doc.pop("_id")   # on utilise enterprise_number comme clé unique

            ops.append(UpdateOne(
                {"enterprise_number": doc["enterprise_number"]},
                {"$set": doc},
                upsert=True,
            ))

        try:
            result = db.hebergements.bulk_write(ops, ordered=False)
            inserted += result.upserted_count
            updated  += result.modified_count
            log.info(
                f"  Lot {skip}–{skip + len(batch)} — "
                f"insérés={result.upserted_count} maj={result.modified_count}"
            )
        except Exception as exc:
            log.error(f"  Erreur bulk_write lot {skip} : {exc}")
            errors += len(ops)

        skip += len(batch)

    total = db.hebergements.count_documents({})
    log.info(
        f"\n✓ Collection 'hebergements' : {total:,} documents\n"
        f"  Insérés : {inserted} | Mis à jour : {updated} | Erreurs : {errors}"
    )

    # Aperçu des codes NACE 55.x présents
    codes = db.hebergements.aggregate([
        {"$unwind": "$activities"},
        {"$match": {"activities.nace_code": {"$regex": "^55"}}},
        {"$group": {
            "_id":   "$activities.nace_code",
            "count": {"$sum": 1},
        }},
        {"$sort": {"count": -1}},
    ])
    log.info("Répartition par code NACE :")
    for c in codes:
        log.info(f"  {c['_id']} : {c['count']:,}")


if __name__ == "__main__":
    run()