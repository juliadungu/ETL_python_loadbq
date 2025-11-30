import io
import zipfile
import requests
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()
DATASET = "gdelt_raw"
TABLE = "gdelt_events"

def download_and_extract(url: str) -> pd.DataFrame:
    resp = requests.get(url)
    resp.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    name = zf.namelist()[0]
    with zf.open(name) as f:
        df = pd.read_csv(f, sep="\t", header=None)
    # optionally assign column names according to GDELT schema
    return df

def load_to_bq(df: pd.DataFrame):
    table_id = f"{client.project}.{DATASET}.{TABLE}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()

def run_once(url: str):
    df = download_and_extract(url)
    load_to_bq(df)

if __name__ == "__main__":
    example_url = "http://data.gdeltproject.org/events/20251130000000.export.CSV.zip"
    run_once(example_url)
