"""
state_db.py
-----------
Toutes les opérations sur la collection download_state (State DB).

Schéma d'un document download_state :
{
    "enterprise_number": "0878.065.378",
    "source":            "cbso" | "ejustice" | "stapor",
    "deposit_id":        "uuid-ou-numac-ou-doc-id",
    "year":              2023,              # None pour ejustice/stapor
    "file_type":         "pdf" | "csv" | "json",
    "status":            "pending" | "done" | "error",
    "hdfs_path":         "/data/bronze/...",
    "size_bytes":        123456,
    "retry_count":       0,
    "error_message":     None,
    "created_at":        datetime,
    "downloaded_at":     datetime | None,
}
"""

import logging
from datetime import datetime, timezone
from typing import Literal

from pymongo import UpdateOne
from pymongo.database import Database

from db.mongo_client import get_db

log = logging.getLogger(__name__)

Source   = Literal["cbso", "ejustice", "stapor"]
Status   = Literal["pending", "done", "error"]
FileType = Literal["pdf", "csv", "json"]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _key(enterprise_number: str, source: Source, deposit_id: str, file_type: FileType) -> dict:
    return {
        "enterprise_number": enterprise_number,
        "source":            source,
        "deposit_id":        deposit_id,
        "file_type":         file_type,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Lecture
# ─────────────────────────────────────────────────────────────────────────────

def _col(db: Database, collection_name: str):
    """Retourne la collection MongoDB selon le nom donné."""
    return db[collection_name]


def is_done(
    enterprise_number: str,
    source: Source,
    deposit_id: str,
    file_type: FileType,
    db: Database | None = None,
    collection_name: str = "download_state",
) -> bool:
    """Retourne True si ce fichier a déjà été téléchargé avec succès."""
    db = db or get_db()
    doc = _col(db, collection_name).find_one(
        {**_key(enterprise_number, source, deposit_id, file_type), "status": "done"},
        {"_id": 1},
    )
    return doc is not None


def get_delta(
    enterprise_number: str,
    source: Source,
    all_deposit_ids: list[str],
    file_type: FileType,
    db: Database | None = None,
    collection_name: str = "download_state",
) -> list[str]:
    """
    Retourne uniquement les deposit_ids qui ne sont PAS encore 'done'.
    C'est le delta à télécharger.
    """
    db = db or get_db()
    done_ids = set(
        doc["deposit_id"]
        for doc in _col(db, collection_name).find(
            {
                "enterprise_number": enterprise_number,
                "source":            source,
                "file_type":         file_type,
                "status":            "done",
                "deposit_id":        {"$in": all_deposit_ids},
            },
            {"deposit_id": 1},
        )
    )
    delta = [d for d in all_deposit_ids if d not in done_ids]
    log.info(
        f"[StateDB] Delta {enterprise_number}/{source}/{file_type} : "
        f"{len(delta)}/{len(all_deposit_ids)} à télécharger"
    )
    return delta


def get_stats(
    enterprise_number: str,
    db: Database | None = None,
    collection_name: str = "download_state",
) -> dict:
    """Résumé de l'état d'ingestion pour une entreprise."""
    db = db or get_db()
    pipeline = [
        {"$match": {"enterprise_number": enterprise_number}},
        {"$group": {
            "_id":   {"source": "$source", "file_type": "$file_type", "status": "$status"},
            "count": {"$sum": 1},
        }},
    ]
    stats: dict = {}
    for row in _col(db, collection_name).aggregate(pipeline):
        key = f"{row['_id']['source']}/{row['_id']['file_type']}/{row['_id']['status']}"
        stats[key] = row["count"]
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Écriture
# ─────────────────────────────────────────────────────────────────────────────

def mark_pending(
    enterprise_number: str,
    source: Source,
    deposit_id: str,
    file_type: FileType,
    year: int | None = None,
    db: Database | None = None,
    collection_name: str = "download_state",
) -> None:
    """Enregistre un fichier comme 'pending' (si pas déjà présent)."""
    db = db or get_db()
    _col(db, collection_name).update_one(
        _key(enterprise_number, source, deposit_id, file_type),
        {"$setOnInsert": {
            **_key(enterprise_number, source, deposit_id, file_type),
            "year":          year,
            "status":        "pending",
            "hdfs_path":     None,
            "size_bytes":    None,
            "retry_count":   0,
            "error_message": None,
            "created_at":    _now(),
            "downloaded_at": None,
        }},
        upsert=True,
    )


def mark_done(
    enterprise_number: str,
    source: Source,
    deposit_id: str,
    file_type: FileType,
    hdfs_path: str,
    size_bytes: int,
    db: Database | None = None,
    collection_name: str = "download_state",
) -> None:
    """Marque un fichier comme téléchargé avec succès."""
    db = db or get_db()
    _col(db, collection_name).update_one(
        _key(enterprise_number, source, deposit_id, file_type),
        {"$set": {
            "status":        "done",
            "hdfs_path":     hdfs_path,
            "size_bytes":    size_bytes,
            "error_message": None,
            "downloaded_at": _now(),
        }},
        upsert=True,
    )
    log.info(f"[StateDB] done → {enterprise_number}/{source}/{file_type} : {hdfs_path}")


def mark_error(
    enterprise_number: str,
    source: Source,
    deposit_id: str,
    file_type: FileType,
    error_message: str,
    db: Database | None = None,
    collection_name: str = "download_state",
) -> None:
    """Marque un fichier en erreur et incrémente le retry_count."""
    db = db or get_db()
    _col(db, collection_name).update_one(
        _key(enterprise_number, source, deposit_id, file_type),
        {
            "$set": {"status": "error", "error_message": error_message[:500]},
            "$inc": {"retry_count": 1},
        },
        upsert=True,
    )
    log.warning(f"[StateDB] error → {enterprise_number}/{source}/{deposit_id} : {error_message[:80]}")


def bulk_mark_pending(
    records: list[dict],
    db: Database | None = None,
    collection_name: str = "download_state",
) -> int:
    """
    Insertion bulk de records pending (pour initialiser un batch).

    Chaque record doit contenir :
      enterprise_number, source, deposit_id, file_type, year (optionnel)
    """
    db = db or get_db()
    if not records:
        return 0

    ops = [
        UpdateOne(
            _key(r["enterprise_number"], r["source"], r["deposit_id"], r["file_type"]),
            {"$setOnInsert": {
                **_key(r["enterprise_number"], r["source"], r["deposit_id"], r["file_type"]),
                "year":          r.get("year"),
                "status":        "pending",
                "hdfs_path":     None,
                "size_bytes":    None,
                "retry_count":   0,
                "error_message": None,
                "created_at":    _now(),
                "downloaded_at": None,
            }},
            upsert=True,
        )
        for r in records
    ]

    result = _col(db, collection_name).bulk_write(ops, ordered=False)
    log.info(f"[StateDB] bulk_pending : {result.upserted_count} nouveaux, {result.matched_count} existants")
    return result.upserted_count