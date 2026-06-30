"""
init_mongodb.py
peuple MongoDB depuis les CSV KBO.

"""

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pymongo import UpdateOne

from db.mongo_client import get_db, init_indexes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_KBO_PATH = os.getenv(
    "KBO_PATH",
    "/Users/emiliedelrue/Documents/Cours/Archi big data/KboOpenData_0404_2026_06_28_Full",
)

BATCH_SIZE  = 5_000
CHUNK_SIZE  = 200_000   


def format_num_vec(series: pd.Series) -> pd.Series:
    """Vectorisé : XXXX.XXX.XXX — sans apply() ligne par ligne."""
    clean = series.astype(str).str.replace(".", "", regex=False).str.zfill(10)
    return clean.str[:4] + "." + clean.str[4:7] + "." + clean.str[7:]


def _load_csv_full(path: Path, **kwargs) -> pd.DataFrame:
    """Charge un petit CSV entièrement."""
    try:
        return pd.read_csv(path, dtype=str, **kwargs)
    except FileNotFoundError:
        log.warning(f"  Fichier introuvable : {path.name}")
        return pd.DataFrame()


def _load_csv_filtered(
    path: Path,
    entity_col: str,
    known_nums: set,
    **kwargs,
) -> pd.DataFrame:
    """
    Lit un grand CSV en chunks et ne garde que les lignes dont
    entity_col (une fois formaté) est dans known_nums.
    Évite de charger l'intégralité du fichier en mémoire.
    """
    chunks = []
    try:
        for chunk in pd.read_csv(path, dtype=str, chunksize=CHUNK_SIZE, **kwargs):
            chunk["enterprise_number"] = format_num_vec(chunk[entity_col])
            filtered = chunk[chunk["enterprise_number"].isin(known_nums)]
            if not filtered.empty:
                chunks.append(filtered)
    except FileNotFoundError:
        log.warning(f"  Fichier introuvable : {path.name}")
        return pd.DataFrame()

    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def load_kbo_data(kbo_path: str) -> pd.DataFrame:
    p = Path(kbo_path)
    log.info(f"Chargement KBO depuis : {p}")

    # ── enterprise.csv 
    df_ent = _load_csv_full(p / "enterprise.csv")
    if df_ent.empty:
        raise FileNotFoundError(f"enterprise.csv introuvable dans {p}")

    df_ent = df_ent.rename(columns={
        "EnterpriseNumber": "enterprise_number",
        "Status":           "status",
        "JuridicalForm":    "legal_form",
        "StartDate":        "start_date",
    })
    df_ent["enterprise_number"] = format_num_vec(df_ent["enterprise_number"])
    known_nums = set(df_ent["enterprise_number"])
    log.info(f"  enterprise.csv : {len(df_ent):,} lignes — {len(known_nums):,} numéros uniques")

    # ── denomination.csv 
    df_den = _load_csv_filtered(p / "denomination.csv", "EntityNumber", known_nums)
    if not df_den.empty:
        df_den = (
            df_den[
                df_den["Language"].isin(["1", "2"])
                & (df_den["TypeOfDenomination"] == "001")
            ]
            .sort_values("Language", ascending=True)  
            .drop_duplicates("enterprise_number", keep="first")
            [["enterprise_number", "Denomination"]]
            .rename(columns={"Denomination": "name"})
        )
        df_ent = df_ent.merge(df_den, on="enterprise_number", how="left")
        log.info(f"  denomination.csv : {len(df_den):,} noms fusionnés")

    # ── address.csv 
    df_addr = _load_csv_filtered(p / "address.csv", "EntityNumber", known_nums)
    if not df_addr.empty:
        addr_cols = {
            "Zipcode":        "zip",
            "MunicipalityFR": "city_fr",
            "MunicipalityNL": "city_nl",
            "StreetFR":       "street_fr",
            "HouseNumber":    "house_number",
        }
        df_addr = (
            df_addr
            .drop_duplicates("enterprise_number", keep="first")
            .rename(columns=addr_cols)
            [["enterprise_number"] + list(addr_cols.values())]
        )
        df_ent = df_ent.merge(df_addr, on="enterprise_number", how="left")
        log.info(f"  address.csv : {len(df_addr):,} adresses fusionnées")

    # ── contact.csv 
    df_contact = _load_csv_filtered(p / "contact.csv", "EntityNumber", known_nums)
    if not df_contact.empty:
        for contact_type, col_name in [("TEL", "telephone"), ("EMAIL", "email"), ("WEB", "website")]:
            sub = (
                df_contact[df_contact["ContactType"] == contact_type]
                [["enterprise_number", "Value"]]
                .drop_duplicates("enterprise_number", keep="first")
                .rename(columns={"Value": col_name})
            )
            df_ent = df_ent.merge(sub, on="enterprise_number", how="left")
        log.info(f"  contact.csv : contacts fusionnés")

    log.info(f"DataFrame final : {len(df_ent):,} entreprises, {len(df_ent.columns)} colonnes")
    return df_ent, known_nums



def load_activities(kbo_path: str, known_nums: set) -> None:
    p = Path(kbo_path)
    db = get_db()
    now = datetime.now(timezone.utc)

    activity_group_labels: dict[str, str] = {}
    df_codes = _load_csv_full(p / "code.csv")
    if not df_codes.empty:
        fr_codes = df_codes[
            (df_codes["Category"] == "ActivityGroup") & (df_codes["Language"] == "FR")
        ]
        activity_group_labels = dict(zip(fr_codes["Code"], fr_codes["Description"]))
        log.info(f"  code.csv : {len(activity_group_labels)} libellés ActivityGroup")

    df_act = _load_csv_filtered(p / "activity.csv", "EntityNumber", known_nums)
    if df_act.empty:
        log.warning("  activity.csv introuvable ou vide après filtre — skip")
        return

    df_act = df_act.sort_values("NaceVersion", ascending=False)
    log.info(f"  activity.csv : {len(df_act):,} lignes, {df_act['enterprise_number'].nunique():,} entreprises")

    ops = []
    count = 0
    for num, grp in df_act.groupby("enterprise_number"):
        activities = [
            {
                "activity_group_code":  str(row.get("ActivityGroup", "")).zfill(3),
                "activity_group_label": activity_group_labels.get(
                    str(row.get("ActivityGroup", "")).zfill(3), ""
                ),
                "nace_version":    str(row.get("NaceVersion", "")),
                "nace_code":       str(row.get("NaceCode", "")),
                "classification":  str(row.get("Classification", "")),
            }
            for _, row in grp.iterrows()
        ]
        ops.append(UpdateOne(
            {"enterprise_number": num},
            {"$set": {"activities": activities, "updated_at": now}},
        ))
        count += 1

        if len(ops) >= BATCH_SIZE:
            db.enterprises.bulk_write(ops, ordered=False)
            log.info(f"  Activités : {count:,} entreprises traitées")
            ops = []

    if ops:
        db.enterprises.bulk_write(ops, ordered=False)
    log.info(f"  ✓ Activités chargées pour {count:,} entreprises")


def load_establishments(kbo_path: str, known_nums: set) -> None:
    p = Path(kbo_path)
    db = get_db()
    now = datetime.now(timezone.utc)

    df = _load_csv_filtered(p / "establishment.csv", "EnterpriseNumber", known_nums)
    if df.empty:
        log.warning("  establishment.csv introuvable ou vide après filtre — skip")
        return

    df = df.rename(columns={
        "EstablishmentNumber": "establishment_number",
        "StartDate":           "start_date",
    })
    df["establishment_number"] = format_num_vec(df["establishment_number"])
    log.info(f"  establishment.csv : {len(df):,} établissements")

    ops = []
    count = 0
    for num, grp in df.groupby("enterprise_number"):
        establishments = grp[["establishment_number", "start_date"]].to_dict("records")
        ops.append(UpdateOne(
            {"enterprise_number": num},
            {"$set": {"establishments": establishments, "updated_at": now}},
        ))
        count += 1
        if len(ops) >= BATCH_SIZE:
            db.enterprises.bulk_write(ops, ordered=False)
            log.info(f"  Établissements : {count:,} entreprises traitées")
            ops = []

    if ops:
        db.enterprises.bulk_write(ops, ordered=False)
    log.info(f"  ✓ Établissements embarqués pour {count:,} entreprises")


def upsert_enterprises(df: pd.DataFrame) -> dict:
    db  = get_db()
    now = datetime.now(timezone.utc)
    total_inserted = total_updated = 0

    records = df.where(pd.notna(df), None).to_dict("records")
    batches = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]

    for i, batch in enumerate(batches):
        ops = []
        for row in batch:
            num = row.pop("enterprise_number")
            doc = {k: v for k, v in row.items() if v is not None}
            ops.append(UpdateOne(
                {"enterprise_number": num},
                {"$set": {**doc, "updated_at": now},
                 "$setOnInsert": {"enterprise_number": num, "created_at": now}},
                upsert=True,
            ))
        result = db.enterprises.bulk_write(ops, ordered=False)
        total_inserted += result.upserted_count
        total_updated  += result.modified_count
        log.info(f"  Batch {i+1}/{len(batches)} : {result.upserted_count} insérés, {result.modified_count} MàJ")

    return {"inserted": total_inserted, "updated": total_updated}


def run(kbo_path: str = DEFAULT_KBO_PATH) -> None:
    log.info("=" * 60)
    log.info("INIT MONGODB — Entreprises belges (KBO)")
    log.info("=" * 60)

    init_indexes()

    df, known_nums = load_kbo_data(kbo_path)

    log.info(f"\nInsertion dans MongoDB ({len(df):,} entreprises)...")
    stats = upsert_enterprises(df)

    log.info("\nChargement des activités NACE...")
    load_activities(kbo_path, known_nums)

    log.info("\nChargement des établissements...")
    load_establishments(kbo_path, known_nums)

    db    = get_db()
    total = db.enterprises.count_documents({})
    log.info("\n" + "=" * 60)
    log.info("✓ Terminé")
    log.info(f"  Insérés      : {stats['inserted']:,}")
    log.info(f"  Mis à jour   : {stats['updated']:,}")
    log.info(f"  Total en base : {total:,}")
    log.info(f"  UI MongoDB   → http://localhost:8081")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kbo-path", default=DEFAULT_KBO_PATH)
    args = parser.parse_args()
    run(args.kbo_path)