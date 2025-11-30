# GDELT → BigQuery Loader (pandas + `load_table_from_dataframe`)

This script downloads a **GDELT Events** file, unzips it, loads it into a **pandas DataFrame**, and then writes that DataFrame into a **BigQuery table** using `load_table_from_dataframe`.

It is a **simple, batch-style ELT utility**: it does not handle scheduling, schema evolution, or deduplication by itself. It’s meant as a minimal, readable example or a building block for a larger pipeline (e.g. Airflow / Cloud Composer / Cloud Functions).

---

## What this script actually does

End-to-end, the script:

1. **Downloads** a single GDELT events file from a given HTTP URL  
   - Example: `http://data.gdeltproject.org/events/20251130000000.export.CSV.zip`  
   - The file is a **ZIP** containing a single TAB-delimited CSV with no header row.

2. **Unzips and parses** the CSV into a pandas DataFrame  
   - Uses `zipfile.ZipFile` to open the `.zip` in memory  
   - Reads the CSV with `pandas.read_csv(..., sep="\t", header=None)`  
   - At this stage, the DataFrame has generic integer column names (`0, 1, 2, ...`) unless you manually assign the official GDELT schema column names.

3. **Loads the DataFrame into BigQuery**  
   - Uses `google.cloud.bigquery.Client()`  
   - Calls `client.load_table_from_dataframe(df, table_id)`  
   - `table_id` is `<project_id>.gdelt_raw.gdelt_events` by default (you can change dataset/table in the script).

4. **Terminates**  
   - It does **not**:
     - Check if the data already exists
     - Do any transformations beyond reading the file
     - Partition the table
     - Perform deduplication or incremental logic

Think of it as:  
> “Take this one GDELT events file and append it to that BigQuery table.”

---

## Files

- `gdelt_to_bigquery.py`  
  Main script that:
  - Downloads a ZIP file from a URL  
  - Converts it to a DataFrame  
  - Loads it into BigQuery

---

## Requirements

### Python packages

Install dependencies (for example, using `pip`):

```bash
pip install pandas google-cloud-bigquery requests
