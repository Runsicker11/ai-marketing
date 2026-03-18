"""BigQuery helper functions: load rows, delete-insert, run queries."""

from datetime import date
from google.cloud import bigquery
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_client = None

def get_client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=GCP_PROJECT_ID)
    return _client


def table_ref(table_name: str) -> str:
    return f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"


def load_rows(table_name: str, rows: list[dict], schema: list[bigquery.SchemaField]) -> int:
    """Insert rows into a BigQuery table. Returns number of rows loaded."""
    if not rows:
        log.info(f"No rows to load into {table_name}")
        return 0

    client = get_client()
    ref = table_ref(table_name)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json(rows, ref, job_config=job_config)
    job.result()
    log.info(f"Loaded {len(rows)} rows into {table_name}")
    return len(rows)


def full_replace(table_name: str, rows: list[dict], schema: list[bigquery.SchemaField]) -> int:
    """Truncate table and insert fresh rows. For small reference tables."""
    if not rows:
        log.info(f"No rows to load into {table_name}")
        return 0

    client = get_client()
    ref = table_ref(table_name)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(rows, ref, job_config=job_config)
    job.result()
    log.info(f"Full-replaced {table_name} with {len(rows)} rows")
    return len(rows)


def delete_date_range(table_name: str, date_col: str, start: date, end: date,
                      extra_conditions: dict[str, str] | None = None):
    """Delete rows in a date range (inclusive) for idempotent reload.

    Args:
        extra_conditions: Optional dict of {column: value} for additional
            WHERE clauses (e.g. {"site": "https://example.com/"}).
    """
    client = get_client()
    ref = table_ref(table_name)
    query = f"DELETE FROM `{ref}` WHERE {date_col} BETWEEN @start AND @end"
    params = [
        bigquery.ScalarQueryParameter("start", "DATE", start.isoformat()),
        bigquery.ScalarQueryParameter("end", "DATE", end.isoformat()),
    ]
    if extra_conditions:
        for i, (col, val) in enumerate(extra_conditions.items()):
            param_name = f"cond_{i}"
            query += f" AND {col} = @{param_name}"
            params.append(bigquery.ScalarQueryParameter(param_name, "STRING", val))
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    job = client.query(query, job_config=job_config)
    result = job.result()
    log.info(f"Deleted rows from {table_name} where {date_col} between {start} and {end} ({result.num_dml_affected_rows} rows)")


def run_query(sql: str) -> bigquery.table.RowIterator:
    """Execute arbitrary SQL and return results."""
    client = get_client()
    job = client.query(sql)
    return job.result()
