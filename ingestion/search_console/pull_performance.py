"""Pull search query + page performance data from Google Search Console."""

from datetime import date, datetime, timezone

from ingestion.search_console.auth import get_service, get_site_url
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

# API returns max 25,000 rows per request
_MAX_ROWS = 25000


def pull_performance(start_date: date, end_date: date) -> list[dict]:
    """Fetch query+page performance data from Search Console.

    Args:
        start_date: Start date (inclusive).
        end_date: End date (inclusive).

    Returns:
        List of dicts matching SEARCH_CONSOLE_PERFORMANCE schema.
    """
    service = get_service()
    site_url = get_site_url()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    all_rows = []
    start_row = 0

    while True:
        request_body = {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "dimensions": ["date", "query", "page", "country", "device"],
            "rowLimit": _MAX_ROWS,
            "startRow": start_row,
        }

        response = service.searchanalytics().query(
            siteUrl=site_url, body=request_body
        ).execute()

        rows = response.get("rows", [])
        if not rows:
            break

        for row in rows:
            keys = row.get("keys", [])
            if len(keys) < 5:
                continue

            all_rows.append({
                "query_date": keys[0],  # YYYY-MM-DD
                "query": keys[1],
                "page": keys[2],
                "country": keys[3],
                "device": keys[4],
                "impressions": int(row.get("impressions", 0)),
                "clicks": int(row.get("clicks", 0)),
                "ctr": round(row.get("ctr", 0.0), 6),
                "position": round(row.get("position", 0.0), 2),
                "ingested_at": now_str,
            })

        log.info(f"Fetched {len(rows)} rows (offset {start_row})")

        if len(rows) < _MAX_ROWS:
            break
        start_row += _MAX_ROWS

    log.info(f"Total Search Console rows fetched: {len(all_rows)} "
             f"({start_date} to {end_date})")
    return all_rows
