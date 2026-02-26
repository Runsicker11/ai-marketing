"""Deploy unified views to BigQuery by executing the SQL file."""

from pathlib import Path

from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET, GA4_DATASET
from ingestion.utils.bq_client import get_client
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

SQL_FILE = Path(__file__).parent / "create_views.sql"


def deploy():
    client = get_client()
    dataset_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}"

    sql = SQL_FILE.read_text(encoding="utf-8")
    sql = sql.replace("{dataset}", dataset_ref)
    sql = sql.replace("{ga4_dataset}", GA4_DATASET)

    # Split on CREATE OR REPLACE VIEW to get individual statements
    statements = []
    for chunk in sql.split("CREATE OR REPLACE VIEW"):
        chunk = chunk.strip()
        if not chunk or chunk.startswith("--"):
            continue
        stmt = "CREATE OR REPLACE VIEW " + chunk.rstrip(";")
        statements.append(stmt)

    for stmt in statements:
        # Extract view name for logging
        view_name = stmt.split("`")[1] if "`" in stmt else "unknown"
        log.info(f"Deploying view: {view_name}")
        job = client.query(stmt)
        job.result()
        log.info(f"  -> deployed successfully")

    log.info(f"All {len(statements)} views deployed")


if __name__ == "__main__":
    deploy()
