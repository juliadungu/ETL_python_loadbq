#!/usr/bin/env python
"""
main.py

GDELT → BigQuery ETL (Extract, Transform, Load)

Steps:
1. Extract: download a GDELT events .CSV.zip file from HTTP
2. Transform: assign column names, cast types, parse dates, keep useful subset
3. Load: append the cleaned data into a BigQuery table via load_table_from_dataframe

Example usage:

    python main.py \
        --url http://data.gdeltproject.org/events/20251130000000.export.CSV.zip \
        --dataset gdelt_raw \
        --table gdelt_events_clean

Prereqs:
- GOOGLE_APPLICATION_CREDENTIALS set, or running in an authenticated GCP environment
- BigQuery API enabled
"""

import argparse
import io
import logging
import sys
import zipfile
from typing import Optional

import pandas as pd
import requests
from google.cloud import bigquery


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def download_gdelt_zip(url: str) -> bytes:
    """
    Download a GDELT events zip file from the given URL.

    Returns the ZIP content as bytes.
    """
    logging.info("Downloading GDELT file from %s", url)
    resp = requests.get(url)
    resp.raise_for_status()
    logging.info("Downloaded %d bytes", len(resp.content))
    return resp.content


def extract_zip_to_dataframe(zip_bytes: bytes) -> pd.DataFrame:
    """
    Extract the single CSV inside the GDELT ZIP and read it into a pandas DataFrame.

    Assumes:
    - Tab-delimited
    - No header row
    """
    logging.info("Extracting CSV from ZIP")
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    names = zf.namelist()
    if not names:
        raise ValueError("ZIP file contains no entries.")

    csv_name = names[0]
    logging.info("Found CSV file inside ZIP: %s", csv_name)

    with zf.open(csv_name) as f:
        df = pd.read_csv(
            f,
            sep="\t",
            header=None,
            dtype=str,       # read everything as string first; convert later
            low_memory=False,
        )

    logging.info("Loaded raw CSV into DataFrame with %d rows, %d columns", *df.shape)
    return df


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

# Only mapping the columns we actually use.
# GDELT has many more; extend as needed.
COLUMN_MAP = {
    0: "globaleventid",
    1: "sql_date",
    2: "month_year",
    3: "year",
    4: "fraction_date",
    5: "actor1_code",
    6: "actor1_name",
    7: "actor1_country_code",
    13: "actor2_code",
    14: "actor2_name",
    15: "actor2_country_code",
    26: "is_root_event",
    27: "event_code",
    28: "event_root_code",
    29: "quad_class",
    30: "goldstein_scale",
    31: "num_mentions",
    32: "num_articles",
    33: "num_sources",
    34: "avg_tone",
    51: "actiongeo_fullname",
    52: "actiongeo_country_code",
    53: "actiongeo_adm1_code",
    56: "actiongeo_lat",
    57: "actiongeo_lon",
}


def transform_gdelt(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw GDELT DataFrame into a cleaner, typed DataFrame.

    Steps:
    - Rename important columns
    - Cast numeric columns
    - Parse event_date
    - Filter out rows missing id/date
    - Select a subset of useful columns
    """
    logging.info("Transforming raw GDELT DataFrame")

    df = df_raw.copy()

    # Rename selected columns
    df = df.rename(columns=COLUMN_MAP)

    # Ensure required columns exist
    required_cols = [
        "globaleventid",
        "sql_date",
        "year",
        "actor1_code",
        "actor1_name",
        "actor1_country_code",
        "actor2_code",
        "actor2_name",
        "actor2_country_code",
        "event_code",
        "event_root_code",
        "goldstein_scale",
        "num_articles",
        "avg_tone",
        "actiongeo_fullname",
        "actiongeo_country_code",
        "actiongeo_lat",
        "actiongeo_lon",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logging.warning("Missing expected columns (will be filled with NaN): %s", missing)
        for c in missing:
            df[c] = pd.NA

    # Type casting with errors='coerce' to avoid hard failures
    df["globaleventid"] = pd.to_numeric(df["globaleventid"], errors="coerce").astype("Int64")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["goldstein_scale"] = pd.to_numeric(df["goldstein_scale"], errors="coerce").astype("float64")
    df["num_articles"] = pd.to_numeric(df["num_articles"], errors="coerce").astype("Int64")
    df["avg_tone"] = pd.to_numeric(df["avg_tone"], errors="coerce").astype("float64")
    df["actiongeo_lat"] = pd.to_numeric(df["actiongeo_lat"], errors="coerce").astype("float64")
    df["actiongeo_lon"] = pd.to_numeric(df["actiongeo_lon"], errors="coerce").astype("float64")

    # Parse event_date from sql_date (YYYYMMDD)
    df["event_date"] = pd.to_datetime(df["sql_date"], format="%Y%m%d", errors="coerce")

    # Minimal quality filter
    before = len(df)
    df = df[df["globaleventid"].notna() & df["event_date"].notna()]
    after = len(df)
    logging.info("Filtered rows without id/date: %d -> %d", before, after)

    # Select only the columns we care about
    cols_to_keep = [
        "globaleventid",
        "event_date",
        "year",
        "actor1_code",
        "actor1_name",
        "actor1_country_code",
        "actor2_code",
        "actor2_name",
        "actor2_country_code",
        "event_code",
        "event_root_code",
        "goldstein_scale",
        "num_articles",
        "avg_tone",
        "actiongeo_fullname",
        "actiongeo_country_code",
        "actiongeo_lat",
        "actiongeo_lon",
    ]
    df = df[cols_to_keep]

    logging.info("Transformed DataFrame shape: %d rows, %d columns", *df.shape)
    return df


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load_to_bigquery(
    df: pd.DataFrame,
    dataset: str,
    table: str,
    project: Optional[str] = None,
    write_disposition: str = "WRITE_APPEND",
) -> None:
    """
    Load the given DataFrame into a BigQuery table.

    - dataset: BigQuery dataset name (e.g. "gdelt_raw")
    - table: table name (e.g. "gdelt_events_clean")
    - project: optional GCP project ID; if None, uses default
    - write_disposition: WRITE_APPEND | WRITE_TRUNCATE | WRITE_EMPTY
    """
    client = bigquery.Client(project=project)
    table_id = f"{client.project}.{dataset}.{table}"

    logging.info("Loading DataFrame to BigQuery table %s", table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
    )

    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    result = load_job.result()  # Wait for completion

    logging.info(
        "Load completed. Output rows: %s. State: %s",
        result.output_rows,
        load_job.state,
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_once(
    url: str,
    dataset: str,
    table: str,
    project: Optional[str] = None,
    write_disposition: str = "WRITE_APPEND",
) -> None:
    """
    End-to-end ETL:
    - Download GDELT file
    - Extract to pandas DataFrame
    - Transform DataFrame
    - Load into BigQuery
    """
    zip_bytes = download_gdelt_zip(url)
    df_raw = extract_zip_to_dataframe(zip_bytes)
    df_clean = transform_gdelt(df_raw)
    load_to_bigquery(
        df_clean,
        dataset=dataset,
        table=table,
        project=project,
        write_disposition=write_disposition,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="GDELT → BigQuery ETL")
    parser.add_argument(
        "--url",
        required=True,
        help="HTTP URL of a GDELT events .CSV.zip file",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="BigQuery dataset name (e.g. gdelt_raw)",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="BigQuery table name (e.g. gdelt_events_clean)",
    )
    parser.add_argument(
        "--project",
        default=None,
        help="GCP project ID (optional; if omitted, uses environment default)",
    )
    parser.add_argument(
        "--write-disposition",
        choices=["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"],
        default="WRITE_APPEND",
        help="BigQuery write disposition (default: WRITE_APPEND)",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    try:
        run_once(
            url=args.url,
            dataset=args.dataset,
            table=args.table,
            project=args.project,
            write_disposition=args.write_disposition,
        )
    except Exception as e:
        logging.exception("ETL failed: %s", e)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
