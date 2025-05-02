import requests
import sqlite3
import logging
from pathlib import Path
from typing import List, Dict
import csv
import argparse

def extract_launch_data() -> List[dict]:
    """
    Fetch all SpaceX launches from the public API.

    Returns:
        List[dict]: A list of JSON objects representing launches.
    """
    url = "https://api.spacexdata.com/v4/launches"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def extract_rocket_lookup() -> Dict[str, str]:
    """
    Fetch all rocket metadata from the SpaceX API and build a lookup dictionary.

    Returns:
        Dict[str, str]: A mapping from rocket_id to rocket_name.
    """
    url = "https://api.spacexdata.com/v4/rockets"
    response = requests.get(url)
    response.raise_for_status()
    rockets = response.json()
    return {rocket["id"]: rocket["name"] for rocket in rockets}

def transform_data(launches: List[dict], rocket_lookup: Dict[str, str]) -> List[Dict[str, str]]:
    """
    Transform launch data to a flat format and replace rocket IDs with rocket names.

    Args:
        launches (List[dict]): Raw launch data from API.
        rocket_lookup (Dict[str, str]): Mapping from rocket IDs to rocket names.

    Returns:
        List[Dict[str, str]]: Transformed list of launch records.
    """
    transformed = []
    for launch in launches:
        transformed.append({
            "id": launch.get("id"),
            "name": launch.get("name"),
            "date_utc": launch.get("date_utc"),
            "success": launch.get("success"),
            "rocket_name": rocket_lookup.get(launch.get("rocket"), "Unknown"),
            "details": launch.get("details")
        })
    return transformed

def load_to_sqlite(clean_data: List[Dict[str, str]], db_path: str):
    """
    Load transformed data into an SQLite database.

    Args:
        clean_data (List[Dict[str, str]]): Transformed launch data.
        db_path (str): Path to SQLite DB file.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS launches (
            id TEXT PRIMARY KEY,
            name TEXT,
            date_utc TEXT,
            success BOOLEAN,
            rocket_name TEXT,
            details TEXT
        )
    """)

    for row in clean_data:
        cursor.execute("""
            INSERT OR REPLACE INTO launches (id, name, date_utc, success, rocket_name, details)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row["id"],
            row["name"],
            row["date_utc"],
            row["success"],
            row["rocket_name"],
            row["details"]
        ))

    conn.commit()
    conn.close()
    logging.info(f"ETL complete. Data loaded into {db_path}")

def export_to_csv(clean_data: List[Dict[str, str]], output_path: str):
    """
    Export transformed data to a CSV file.

    Args:
        clean_data (List[Dict[str, str]]): Transformed data.
        output_path (str): File path to export the CSV.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=clean_data[0].keys())
        writer.writeheader()
        writer.writerows(clean_data)
    logging.info(f"Transformed data exported to {output_path}")

def parse_args():
    parser = argparse.ArgumentParser(description="ETL pipeline for SpaceX launch data")
    parser.add_argument("--db-path", default="db/spacex.db", help="Path to the SQLite database")
    parser.add_argument("--export-csv", default="data/spacex_launches.csv", help="Path to export CSV data")
    parser.add_argument("--dry-run", action="store_true", help="Only print transformed data, don’t write to DB")
    return parser.parse_args()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    db_path = (script_dir / args.db_path).resolve()
    csv_path = (script_dir / args.export_csv).resolve()

    launches = extract_launch_data()
    rocket_lookup = extract_rocket_lookup()
    transformed_data = transform_data(launches, rocket_lookup)

    export_to_csv(transformed_data, csv_path)

    if args.dry_run:
        logging.info("Dry run mode enabled. Skipping database write.")
    else:
        load_to_sqlite(transformed_data, db_path=db_path)