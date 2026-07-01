#!/usr/bin/env python3
"""
normalize_hebergements.py
--------------------------
Couche Silver : normalise et enrichit la collection MongoDB 'hebergements'.

Transformations (conformes à jour2_instructions.docx) :
  1. Dates         : start_date DD-MM-YYYY → YYYY-MM-DD
  2. Activités     : dédoublonnage sur (nace_code, classification) exact
  3. Adresses      : conserver uniquement type_of_address = "REGO"
  4. Dénominations : type_of_denomination = "1" (nom officiel) en premier
  5. Labels        : ajouter status_label, juridical_form_label, nace_label

Usage :
  python normalize_hebergements.py
  python normalize_hebergements.py --dry-run
  python normalize_hebergements.py --collection hebergements --batch-size 200
  python normalize_hebergements.py --codes-csv /data/kbo/code.csv --nace-csv /data/kbo/nace.csv

Notes :
  - Le script met à jour les documents en place dans la collection (pas de nouvelle collection).
  - Les codes originaux (status, juridical_form, nace_code) sont CONSERVÉS.
    On ajoute uniquement les champs *_label à côté.
  - Compatible avec les noms de champs snake_case (convention du projet)
    et PascalCase (KBO CSV brut) — le script teste les deux.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "dags"))

from pymongo import UpdateOne
from db.mongo_client import get_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Mappings de labels (FR)
# ─────────────────────────────────────────────────────────────────────────────

# NACE labels — secteur 55 complet + activités secondaires courantes
NACE_LABELS: dict[str, str] = {
    # ── Secteur 55 : Hébergement ──────────────────────────────────────────────
    "55":    "Hébergement",
    "55100": "Hôtels et hébergement similaire",
    "55201": "Auberges de jeunesse",
    "55202": "Centres et villages de vacances",
    "55203": "Gîtes de vacances, appartements et meublés de vacances",
    "55204": "Chambres d'hôtes",
    "55209": "Autres hébergements de courte durée n.c.a.",
    "55300": "Terrains de camping et parcs pour caravanes",
    "55400": "Intermédiation pour l'hébergement (type Airbnb/Booking)",
    "55900": "Autres hébergements",
    # ── Secteur 56 : Restauration (fréquent en secondaire) ───────────────────
    "56":    "Restauration",
    "56101": "Restaurants à service complet",
    "56102": "Restaurants à service limité",
    "56210": "Traiteurs et autres services de repas",
    "56290": "Autres services de restauration",
    "56301": "Cafés et bars",
    "56302": "Débits de boissons avec spectacle",
    # ── Immobilier / gestion ──────────────────────────────────────────────────
    "68100": "Activités des marchands de biens immobiliers",
    "68200": "Location et exploitation de biens immobiliers propres ou loués",
    "68311": "Agences immobilières",
    "68312": "Administration de biens immobiliers pour le compte de tiers",
    # ── Gestion / conseil ─────────────────────────────────────────────────────
    "70100": "Activités des sièges sociaux",
    "70200": "Activités des sièges sociaux (Nace2025)",
    "70220": "Conseil pour les affaires et autres conseils de gestion",
    # ── Commerce ──────────────────────────────────────────────────────────────
    "47111": "Commerce de détail en magasin non spécialisé à prédominance alimentaire",
    "47191": "Autre commerce de détail en magasin non spécialisé",
    # ── Autres services ───────────────────────────────────────────────────────
    "96090": "Autres services personnels n.c.a.",
    "93110": "Gestion d'installations sportives",
    "93130": "Activités des centres de bien-être physique",
    "93292": "Autres activités récréatives et de loisirs n.c.a.",
}

# Status labels
STATUS_LABELS: dict[str, str] = {
    "AC":   "Actif",
    "STOP": "Arrêté",
    "NST":  "Non démarré",
    "DIS":  "Dissous",
    "FAI":  "Faillite",
}

# Formes juridiques belges (codes KBO)
# Étendre via --codes-csv pour la liste complète
JURIDICAL_FORM_LABELS: dict[str, str] = {
    # ── Personnes physiques ───────────────────────────────────────────────────
    "1":   "Personne physique",
    "647": "Personne physique (indépendant)",
    "648": "Profession libérale",
    "649": "Exploitation agricole familiale",
    # ── Sociétés (formes post-réforme CSA 2019) ───────────────────────────────
    "601": "Société anonyme (SA)",
    "602": "Société en commandite (SComm)",
    "603": "Société à responsabilité limitée (SRL)",
    "604": "Société coopérative (SC)",
    "605": "Société en nom collectif (SNC)",
    "606": "Société simple (SS)",
    "609": "Groupement européen d'intérêt économique (GEIE)",
    "610": "Société à responsabilité limitée (SRL)",
    "611": "Société coopérative (SC)",
    "614": "Société coopérative agréée (SCA)",
    "617": "Société anonyme (SA)",
    "621": "Société en commandite (SComm)",
    "622": "Société en commandite par actions",
    "624": "Société en participation",
    "625": "Société momentanée",
    "630": "Groupement d'intérêt économique (GIE)",
    "640": "Association momentanée",
    "645": "Société agricole",
    "646": "Groupement forestier",
    # ── ASBL / fondations ─────────────────────────────────────────────────────
    "650": "Association sans but lucratif (ASBL)",
    "651": "Association internationale sans but lucratif (AISBL)",
    "652": "Fondation",
    "653": "Association de fait",
    "655": "Association de copropriétaires",
    # ── Autres ───────────────────────────────────────────────────────────────
    "660": "Société de droit public",
    "670": "Société mutualiste",
    "680": "Coopérative agréée par le Conseil National de la Coopération",
    "690": "Société d'assurance mutuelle",
    "700": "Fonds de pension",
    # ── Entités publiques (exclues du scraping mais présentes en base) ────────
    "110": "Société de droit public",
    "114": "Intercommunale",
    "116": "Association de communes",
    "117": "Établissement public",
    "301": "Service public fédéral",
    "302": "Service public de programmation",
    "303": "Organisme d'intérêt public",
    "310": "Parlement fédéral",
    "320": "Parlement régional ou communautaire",
    "330": "Gouvernement fédéral",
    "340": "Gouvernement régional ou communautaire",
    "350": "Haute administration de l'État",
    "400": "Commune",
    "411": "Centre public d'action sociale (CPAS)",
    "412": "Association de CPAS",
    "413": "Zone de police",
    "414": "Zone de secours",
    "415": "Fabrique d'église",
    "416": "Intercommunale",
    "417": "Société de logement de service public",
    "418": "Régie communale autonome",
    "419": "Association de communes de fait",
    "420": "Province",
}


# ─────────────────────────────────────────────────────────────────────────────
# Chargement CSV optionnel
# ─────────────────────────────────────────────────────────────────────────────

def load_csv_mapping(path: Path, code_col: str, desc_col: str, lang_col: Optional[str] = None) -> dict[str, str]:
    """
    Charge un CSV KBO ou NACE → dict {code: description FR}.

    Colonnes attendues (insensibles à la casse) :
      code_col  : colonne du code  (ex. "Code", "NaceCode")
      desc_col  : colonne du label (ex. "Description", "OmschrijvingFR")
      lang_col  : colonne langue optionnelle — si présente, filtre sur "1" ou "FR"
    """
    mapping: dict[str, str] = {}
    if not path or not path.exists():
        log.warning(f"[csv] Fichier non trouvé : {path}")
        return mapping
    try:
        with path.open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            # Normaliser les noms de colonnes
            headers = {h.strip(): h.strip() for h in (reader.fieldnames or [])}
            for row in reader:
                # Filtre langue
                if lang_col:
                    lc = str(row.get(lang_col, "")).strip()
                    if lc not in ("1", "FR", "fr", ""):
                        continue
                code = str(row.get(code_col, "")).strip()
                desc = str(row.get(desc_col, "")).strip()
                if code and desc:
                    mapping[code] = desc
        log.info(f"[csv] {len(mapping)} entrées chargées depuis {path.name}")
    except Exception as exc:
        log.warning(f"[csv] Erreur lecture {path}: {exc}")
    return mapping


# ─────────────────────────────────────────────────────────────────────────────
# Transformations unitaires
# ─────────────────────────────────────────────────────────────────────────────

def normalize_date(raw: object) -> object:
    """
    Convertit DD-MM-YYYY (ou DD/MM/YYYY) → YYYY-MM-DD.
    Retourne la valeur d'origine si non reconnue ou déjà au bon format.
    """
    if not isinstance(raw, str):
        return raw
    s = raw.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s  # déjà normalisé
    for pattern in (
        r"^(\d{1,2})-(\d{1,2})-(\d{4})$",
        r"^(\d{1,2})/(\d{1,2})/(\d{4})$",
    ):
        m = re.match(pattern, s)
        if m:
            d, mo, y = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
            return f"{y}-{mo}-{d}"
    return raw  # format inconnu — on ne touche pas


def _get(doc: dict, *keys: str) -> object:
    """Retourne la première valeur trouvée parmi les clés (snake_case / PascalCase)."""
    for k in keys:
        if k in doc:
            return doc[k]
    return None


def deduplicate_activities(activities: list) -> list:
    """
    Supprime les doublons stricts : même nace_code + même classification.
    - 62020 MAIN x2 → garde 1
    - 70220 MAIN + 70200 MAIN → garde les deux (codes différents)
    - MAIN + SECO du même code → garde les deux (classifications différentes)
    """
    seen: set[tuple] = set()
    result: list = []
    for act in activities:
        code = str(act.get("nace_code") or act.get("NaceCode") or "").strip()
        cls  = str(act.get("classification") or act.get("Classification") or "").strip()
        key  = (code, cls)
        if key not in seen:
            seen.add(key)
            result.append(act)
    return result


def enrich_activities(activities: list, nace_labels: dict[str, str]) -> list:
    """Ajoute nace_label à chaque activité (si disponible dans le mapping)."""
    result = []
    for act in list(activities):
        act = dict(act)
        code  = str(act.get("nace_code") or act.get("NaceCode") or "").strip()
        label = nace_labels.get(code, "")
        if label:
            # Champ label dans la même convention que nace_code
            if "nace_code" in act:
                act["nace_label"] = label
            else:
                act["NaceLabel"] = label
        result.append(act)
    return result


def filter_rego_address(addresses: list) -> list:
    """
    Conserve uniquement TypeOfAddress = REGO (siège social enregistré).
    Si aucune adresse REGO trouvée, conserve tout (fallback safe).
    """
    rego = [
        a for a in addresses
        if str(a.get("type_of_address") or a.get("TypeOfAddress") or "").strip().upper() == "REGO"
    ]
    return rego or addresses


def order_denominations(denominations: list) -> list:
    """
    Met type_of_denomination = "1" (nom officiel) en tête de liste.
    Les autres dénominations restent dans leur ordre d'origine.
    """
    def _key(d: dict) -> int:
        val = str(d.get("type_of_denomination") or d.get("TypeOfDenomination") or "99").strip()
        return 0 if val == "1" else 1

    return sorted(denominations, key=_key)


# ─────────────────────────────────────────────────────────────────────────────
# Normalisation d'un document complet
# ─────────────────────────────────────────────────────────────────────────────

def normalize_document(
    doc: dict,
    jf_labels: dict[str, str],
    nace_labels: dict[str, str],
) -> dict:
    """
    Calcule le $set à appliquer pour normaliser un document.
    Retourne un dict vide si rien à modifier.
    """
    updates: dict = {}

    # 1. Normalisation de la date de début
    for field in ("start_date", "StartDate"):
        if field in doc:
            normalized = normalize_date(doc[field])
            if normalized != doc[field]:
                updates[field] = normalized
            break  # tester un seul champ

    # 2. Dédoublonnage + enrichissement des activités
    if isinstance(doc.get("activities"), list):
        deduped  = deduplicate_activities(doc["activities"])
        enriched = enrich_activities(deduped, nace_labels)
        # Toujours écrire pour s'assurer que les labels sont à jour
        updates["activities"] = enriched

    # 3. Adresse REGO uniquement
    for field in ("addresses", "Addresses"):
        if isinstance(doc.get(field), list):
            filtered = filter_rego_address(doc[field])
            if filtered != doc[field]:
                updates[field] = filtered
            break

    # 4. Dénominations : officielle en premier
    for field in ("denominations", "Denominations"):
        if isinstance(doc.get(field), list):
            ordered = order_denominations(doc[field])
            updates[field] = ordered
            break

    # 5. Labels

    # Status
    status = str(_get(doc, "status", "Status") or "").strip()
    if status:
        slabel = STATUS_LABELS.get(status, "")
        if slabel:
            updates["status_label"] = slabel

    # Forme juridique
    jf = str(_get(doc, "juridical_form", "JuridicalForm") or "").strip()
    if jf:
        jf_label = jf_labels.get(jf) or JURIDICAL_FORM_LABELS.get(jf, "")
        if jf_label:
            updates["juridical_form_label"] = jf_label

    # Métadonnées Silver
    updates["silver_normalized_at"] = datetime.now(timezone.utc)
    updates["layer"] = "silver"

    return updates


# ─────────────────────────────────────────────────────────────────────────────
# Boucle principale
# ─────────────────────────────────────────────────────────────────────────────

def run(
    collection_name: str = "hebergements",
    dry_run: bool = False,
    batch_size: int = 500,
    codes_csv: Optional[Path] = None,
    nace_csv: Optional[Path] = None,
) -> None:
    db  = get_db()
    col = db[collection_name]

    total = col.count_documents({})
    log.info(f"Collection '{collection_name}' : {total} documents à traiter")

    # Chargement des mappings CSV optionnels
    jf_labels: dict[str, str] = {}
    if codes_csv:
        # code.csv KBO — colonnes : Code, Language, Description
        jf_labels = load_csv_mapping(codes_csv, "Code", "Description", lang_col="Language")

    nace_labels: dict[str, str] = dict(NACE_LABELS)
    if nace_csv:
        # nace.csv — colonnes : NaceCode (ou Code), DescriptionFR (ou Description)
        extra = load_csv_mapping(
            nace_csv,
            code_col=next((c for c in ("NaceCode", "Code") if True), "Code"),
            desc_col=next((c for c in ("DescriptionFR", "Description") if True), "Description"),
        )
        nace_labels.update(extra)

    processed = 0
    modified  = 0
    errors    = 0
    ops: list = []

    cursor = col.find({}, no_cursor_timeout=True).batch_size(batch_size)
    try:
        for doc in cursor:
            try:
                changes = normalize_document(doc, jf_labels, nace_labels)
                if changes:
                    ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": changes}))
                    modified += 1
                processed += 1

                # Flush par lot
                if len(ops) >= batch_size:
                    if not dry_run:
                        col.bulk_write(ops, ordered=False)
                    log.info(
                        f"{'[DRY-RUN] ' if dry_run else ''}Flush {len(ops)} ops — "
                        f"{processed}/{total} traités"
                    )
                    ops = []

            except Exception as exc:
                log.error(
                    f"Erreur doc {doc.get('enterprise_number', str(doc.get('_id', '?')))}: {exc}"
                )
                errors += 1

    finally:
        cursor.close()

    # Dernier flush
    if ops:
        if not dry_run:
            col.bulk_write(ops, ordered=False)
        log.info(
            f"{'[DRY-RUN] ' if dry_run else ''}Flush final {len(ops)} ops"
        )

    log.info(
        "\n"
        f"{'[DRY-RUN] ' if dry_run else ''}"
        f"Normalisation Silver terminée\n"
        f"  Total     : {total}\n"
        f"  Traités   : {processed}\n"
        f"  Modifiés  : {modified}\n"
        f"  Erreurs   : {errors}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Entrée
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Normalisation Silver — collection MongoDB hebergements"
    )
    parser.add_argument(
        "--collection", default="hebergements",
        help="Nom de la collection à normaliser (défaut: hebergements)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Simule sans écrire dans MongoDB",
    )
    parser.add_argument(
        "--batch-size", type=int, default=500,
        help="Nombre de documents par lot (défaut: 500)",
    )
    parser.add_argument(
        "--codes-csv", type=Path, default=None,
        help="CSV KBO des codes (formes juridiques) — colonnes: Code, Language, Description",
    )
    parser.add_argument(
        "--nace-csv", type=Path, default=None,
        help="CSV des labels NACE — colonnes: NaceCode (ou Code), DescriptionFR (ou Description)",
    )
    args = parser.parse_args()

    run(
        collection_name=args.collection,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        codes_csv=args.codes_csv,
        nace_csv=args.nace_csv,
    )