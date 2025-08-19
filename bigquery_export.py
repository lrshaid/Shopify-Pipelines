import os
import re
import sys
from typing import List

from dotenv import load_dotenv
from google.cloud import bigquery

# Load .env if present
load_dotenv()

# Configuration from environment variables
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "your-gcp-project")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "shopify_raw")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "US")
WRITE_DISPOSITION = os.getenv("BIGQUERY_WRITE_DISPOSITION", "WRITE_TRUNCATE")  # WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY

# Directories to scan for JSONL files
SEARCH_DIRS: List[str] = [
    "bulk_data",
    ".",
]


def sanitize_table_name(filename: str) -> str:
    """Convert a filename to a valid BigQuery table name."""
    name = os.path.splitext(os.path.basename(filename))[0]
    # Common suffix cleanup
    name = re.sub(r"_data$", "", name)
    # Replace non-alphanumerics with underscores
    name = re.sub(r"[^A-Za-z0-9_]", "_", name)
    # Ensure starts with letter or underscore
    if not re.match(r"^[A-Za-z_]", name):
        name = f"_{name}"
    # Trim to 1024 chars (BQ limit is higher, but this is safe)
    return name[:1024]


def discover_jsonl_files() -> List[str]:
    files: List[str] = []
    for dir_path in SEARCH_DIRS:
        if not os.path.isdir(dir_path):
            continue
        for entry in os.listdir(dir_path):
            if entry.endswith(".jsonl"):
                files.append(os.path.join(dir_path, entry))
    # De-duplicate while preserving order
    seen = set()
    unique_files: List[str] = []
    for f in files:
        if f not in seen:
            unique_files.append(f)
            seen.add(f)
    return unique_files


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    dataset_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset_ref.location = location
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)


def load_jsonl_file(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    file_path: str,
    write_disposition: str,
) -> None:
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    job_config.write_disposition = write_disposition

    with open(file_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_ref, job_config=job_config)
    result = load_job.result()  # Waits for job to complete

    table = client.get_table(table_ref)
    print(f"Loaded {result.output_rows} rows into {table.project}:{table.dataset_id}.{table.table_id}")


def main() -> None:
    if GOOGLE_CLOUD_PROJECT in (None, "", "your-gcp-project"):
        print("Error: Set GOOGLE_CLOUD_PROJECT env var to your GCP project ID.")
        sys.exit(1)

    if BIGQUERY_DATASET in (None, ""):
        print("Error: Set BIGQUERY_DATASET env var to your BigQuery dataset name.")
        sys.exit(1)

    files = discover_jsonl_files()
    if not files:
        print("No .jsonl files found in 'bulk_data' or project root.")
        sys.exit(0)

    print(f"Project: {GOOGLE_CLOUD_PROJECT}")
    print(f"Dataset: {BIGQUERY_DATASET} (location: {BIGQUERY_LOCATION})")
    print(f"Write disposition: {WRITE_DISPOSITION}")
    print("Files to load:")
    for f in files:
        print(f" - {f}")

    client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT)
    ensure_dataset(client, BIGQUERY_DATASET, BIGQUERY_LOCATION)

    failures = 0
    for path in files:
        table_name = sanitize_table_name(path)
        print("\n==== Loading ====")
        print(f"File: {path}")
        print(f"Table: {table_name}")
        try:
            load_jsonl_file(
                client=client,
                dataset_id=BIGQUERY_DATASET,
                table_id=table_name,
                file_path=path,
                write_disposition=WRITE_DISPOSITION,
            )
        except Exception as exc:
            failures += 1
            print(f"FAILED to load {path}: {exc}")

    if failures:
        print(f"\nCompleted with {failures} failure(s).")
        sys.exit(1)

    print("\nAll files loaded successfully.")


if __name__ == "__main__":
    main()
