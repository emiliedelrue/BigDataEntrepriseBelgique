"""
mongo_client.py
---------------
Connexion MongoDB partagée pour tous les DAGs et scrapers.

Utilisation :
    from db.mongo_client import get_db
    db = get_db()
    db.enterprises.find_one({"enterprise_number": "0878.065.378"})

Collections :
    enterprises     — entreprises belges (source : KBO CSV)
    download_state  — état de chaque fichier téléchargé (State DB)
"""

import os
from functools import lru_cache

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.database import Database

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "belgique")


@lru_cache(maxsize=1)
def get_client() -> MongoClient:
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)


def get_db() -> Database:
    return get_client()[MONGO_DB]


def init_indexes() -> None:
    """
    Crée les index nécessaires (idempotent — safe à appeler plusieurs fois).
    À appeler une seule fois au démarrage (via init_mongodb.py).
    """
    db = get_db()

    # ── enterprises ──────────────────────────────────────────────────────────
    db.enterprises.create_index(
        [("enterprise_number", ASCENDING)], unique=True, name="idx_enterprise_number"
    )
    db.enterprises.create_index(
        [("status", ASCENDING)], name="idx_status"
    )
    db.enterprises.create_index(
        [("legal_form", ASCENDING)], name="idx_legal_form"
    )

    # ── download_state ────────────────────────────────────────────────────────
    # Index composite unique : (entreprise, source, deposit_id, file_type)
    db.download_state.create_index(
        [
            ("enterprise_number", ASCENDING),
            ("source",            ASCENDING),
            ("deposit_id",        ASCENDING),
            ("file_type",         ASCENDING),
        ],
        unique=True,
        name="idx_state_unique",
    )
    db.download_state.create_index(
        [("status", ASCENDING)], name="idx_state_status"
    )
    db.download_state.create_index(
        [("enterprise_number", ASCENDING), ("source", ASCENDING)],
        name="idx_state_enterprise_source",
    )
    db.download_state.create_index(
        [("downloaded_at", DESCENDING)], name="idx_state_date"
    )

    print("✓ Index MongoDB créés")