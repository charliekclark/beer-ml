#!/usr/bin/env python3
"""Convert recipes.jsonl → SQLite (normalized) + CSV (flat recipes table).

Usage:
    python jsonl_to_db.py [--input PATH] [--db PATH] [--csv PATH]

Defaults:
    --input  exports/brewersfriend/recipes.jsonl
    --db     exports/brewersfriend/recipes.db
    --csv    exports/brewersfriend/recipes.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS recipes (
    id                        TEXT PRIMARY KEY,
    url                       TEXT,
    title                     TEXT,
    author                    TEXT,
    brew_method               TEXT,
    style_name                TEXT,
    boil_time                 TEXT,
    batch_size                TEXT,
    boil_size                 TEXT,
    boil_gravity              REAL,
    efficiency                TEXT,
    hop_utilization_multiplier REAL,
    original_gravity          REAL,
    final_gravity             REAL,
    abv_standard              REAL,
    ibu_tinseth               REAL,
    srm_morey                 REAL,
    mash_ph                   REAL,
    yeast_name                TEXT,
    yeast_starter             TEXT,
    yeast_form                TEXT,
    yeast_attenuation         TEXT,
    yeast_flocculation        TEXT,
    yeast_optimum_temp        TEXT,
    yeast_fermentation_temp   TEXT,
    yeast_pitch_rate          TEXT,
    water_profile_name        TEXT,
    water_ca2                 REAL,
    water_mg2                 REAL,
    water_na                  REAL,
    water_cl                  REAL,
    water_so4                 REAL,
    water_hco3                REAL,
    priming_method            TEXT,
    priming_co2_level         TEXT,
    error                     TEXT
);

CREATE TABLE IF NOT EXISTS fermentables (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    recipe_id  TEXT REFERENCES recipes(id),
    amount     TEXT,
    name       TEXT,
    bill_pct   REAL
);

CREATE TABLE IF NOT EXISTS hops (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    recipe_id  TEXT REFERENCES recipes(id),
    amount     TEXT,
    name       TEXT,
    type       TEXT,
    alpha_acid TEXT,
    use        TEXT,
    ibu        TEXT
);

CREATE TABLE IF NOT EXISTS other_ingredients (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    recipe_id  TEXT REFERENCES recipes(id),
    amount     TEXT,
    name       TEXT,
    time       TEXT,
    type       TEXT,
    use        TEXT
);

CREATE TABLE IF NOT EXISTS mash_steps (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    recipe_id   TEXT REFERENCES recipes(id),
    step_order  INTEGER,
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_fermentables_recipe ON fermentables(recipe_id);
CREATE INDEX IF NOT EXISTS idx_hops_recipe         ON hops(recipe_id);
CREATE INDEX IF NOT EXISTS idx_other_recipe        ON other_ingredients(recipe_id);
CREATE INDEX IF NOT EXISTS idx_mash_recipe         ON mash_steps(recipe_id);
"""

CSV_COLUMNS = [
    "id", "url", "title", "author", "brew_method", "style_name",
    "boil_time", "batch_size", "boil_size", "boil_gravity", "efficiency",
    "hop_utilization_multiplier", "original_gravity", "final_gravity",
    "abv_standard", "ibu_tinseth", "srm_morey", "mash_ph",
    "yeast_name", "yeast_starter", "yeast_form", "yeast_attenuation",
    "yeast_flocculation", "yeast_optimum_temp", "yeast_fermentation_temp",
    "yeast_pitch_rate",
    "water_profile_name", "water_ca2", "water_mg2", "water_na",
    "water_cl", "water_so4", "water_hco3",
    "priming_method", "priming_co2_level",
    "error",
]


def _float(val: object) -> float | None:
    if val is None:
        return None
    s = re.sub(r"[^0-9.\-]", "", str(val))
    try:
        return float(s)
    except ValueError:
        return None


def parse_record(r: dict) -> tuple[dict, list, list, list, list]:
    yeast = r.get("yeast") or {}
    attenuation = (
        yeast.get("attenuation_avg")
        or yeast.get("attenuation_custom")
        or yeast.get("apparent_attenuation")
    )

    water = r.get("water_profile") or {}
    priming = r.get("priming") or {}

    recipe_row = {
        "id": str(r.get("id", "")),
        "url": r.get("url"),
        "title": r.get("title"),
        "author": r.get("author"),
        "brew_method": r.get("brew_method"),
        "style_name": r.get("style_name"),
        "boil_time": r.get("boil_time"),
        "batch_size": r.get("batch_size"),
        "boil_size": r.get("boil_size"),
        "boil_gravity": _float(r.get("boil_gravity")),
        "efficiency": r.get("efficiency"),
        "hop_utilization_multiplier": _float(r.get("hop_utilization_multiplier")),
        "original_gravity": _float(r.get("original_gravity")),
        "final_gravity": _float(r.get("final_gravity")),
        "abv_standard": _float(r.get("abv_standard")),
        "ibu_tinseth": _float(r.get("ibu_tinseth")),
        "srm_morey": _float(r.get("srm_morey")),
        "mash_ph": _float(r.get("mash_ph")),
        "yeast_name": yeast.get("name"),
        "yeast_starter": yeast.get("starter"),
        "yeast_form": yeast.get("form"),
        "yeast_attenuation": attenuation,
        "yeast_flocculation": yeast.get("flocculation"),
        "yeast_optimum_temp": yeast.get("optimum_temp"),
        "yeast_fermentation_temp": yeast.get("fermentation_temp"),
        "yeast_pitch_rate": yeast.get("pitch_rate"),
        "water_profile_name": water.get("profile_name"),
        "water_ca2": _float(water.get("ca2")),
        "water_mg2": _float(water.get("mg2")),
        "water_na": _float(water.get("na")),
        "water_cl": _float(water.get("cl")),
        "water_so4": _float(water.get("so4")),
        "water_hco3": _float(water.get("hco3")),
        "priming_method": priming.get("method"),
        "priming_co2_level": priming.get("co2_level"),
        "error": r.get("error"),
    }

    rid = recipe_row["id"]

    fermentables = [
        {
            "recipe_id": rid,
            "amount": f.get("amount"),
            "name": f.get("name"),
            "bill_pct": _float(f.get("bill_pct")),
        }
        for f in (r.get("fermentables") or [])
    ]

    hops = [
        {
            "recipe_id": rid,
            "amount": h.get("amount"),
            "name": h.get("name"),
            "type": h.get("type"),
            "alpha_acid": h.get("alpha_acid"),
            "use": h.get("use"),
            "ibu": h.get("ibu"),
        }
        for h in (r.get("hops") or [])
    ]

    other = [
        {
            "recipe_id": rid,
            "amount": o.get("amount"),
            "name": o.get("name"),
            "time": o.get("time"),
            "type": o.get("type"),
            "use": o.get("use"),
        }
        for o in (r.get("other_ingredients") or [])
    ]

    mash = [
        {"recipe_id": rid, "step_order": idx + 1, "description": step}
        for idx, step in enumerate(r.get("mash_steps") or [])
    ]

    return recipe_row, fermentables, hops, other, mash


def convert(input_path: Path, db_path: Path, csv_path: Path) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)

    existing_ids: set[str] = {
        row[0] for row in conn.execute("SELECT id FROM recipes")
    }

    inserted = 0
    skipped = 0

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_exists = csv_path.exists()
    csv_fh = csv_path.open("a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    if not csv_exists:
        writer.writeheader()

    with input_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            recipe_row, fermentables, hops, other, mash = parse_record(record)
            rid = recipe_row["id"]

            if rid in existing_ids:
                skipped += 1
                continue

            conn.execute(
                f"INSERT OR IGNORE INTO recipes ({','.join(recipe_row.keys())}) "
                f"VALUES ({','.join('?' * len(recipe_row))})",
                list(recipe_row.values()),
            )
            if fermentables:
                conn.executemany(
                    "INSERT INTO fermentables (recipe_id,amount,name,bill_pct) VALUES (?,?,?,?)",
                    [(f["recipe_id"], f["amount"], f["name"], f["bill_pct"]) for f in fermentables],
                )
            if hops:
                conn.executemany(
                    "INSERT INTO hops (recipe_id,amount,name,type,alpha_acid,use,ibu) "
                    "VALUES (?,?,?,?,?,?,?)",
                    [(h["recipe_id"], h["amount"], h["name"], h["type"],
                      h["alpha_acid"], h["use"], h["ibu"]) for h in hops],
                )
            if other:
                conn.executemany(
                    "INSERT INTO other_ingredients (recipe_id,amount,name,time,type,use) "
                    "VALUES (?,?,?,?,?,?)",
                    [(o["recipe_id"], o["amount"], o["name"], o["time"],
                      o["type"], o["use"]) for o in other],
                )
            if mash:
                conn.executemany(
                    "INSERT INTO mash_steps (recipe_id,step_order,description) VALUES (?,?,?)",
                    [(m["recipe_id"], m["step_order"], m["description"]) for m in mash],
                )

            writer.writerow(recipe_row)
            existing_ids.add(rid)
            inserted += 1

            if inserted % 5000 == 0:
                conn.commit()
                print(f"  {inserted} recipes inserted...", flush=True)

    conn.commit()
    conn.close()
    csv_fh.close()

    total = inserted + skipped
    print(
        f"Done. {inserted} new recipes added ({skipped} already present). "
        f"Total in DB: {total}",
        flush=True,
    )
    print(f"  SQLite: {db_path}", flush=True)
    print(f"  CSV:    {csv_path}", flush=True)
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert recipes.jsonl to SQLite + CSV.")
    parser.add_argument("--input", default="exports/brewersfriend/recipes.jsonl")
    parser.add_argument("--db", default="exports/brewersfriend/recipes.db")
    parser.add_argument("--csv", default="exports/brewersfriend/recipes.csv")
    args = parser.parse_args()

    return 0 if convert(Path(args.input), Path(args.db), Path(args.csv)) >= 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
