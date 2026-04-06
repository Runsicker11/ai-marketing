"""Bulk SEO meta optimizer — generates and applies Yoast title/description proposals.

Workflow:
    1. propose() — pulls WP content + Search Console data, generates optimized
                   meta via Claude, saves review file to reports/
    2. apply()   — reads the saved review file and pushes approved proposals
                   to WordPress via the REST API

Usage:
    uv run python -m seo.run --optimize-meta --print
    uv run python -m seo.run --apply-meta-proposals
"""

import json
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests
import yaml

from ingestion.analysis.claude_client import analyze
from ingestion.utils.bq_client import run_query
from ingestion.utils.config import GCP_PROJECT_ID, BQ_DATASET
from ingestion.utils.logger import get_logger
from seo.wordpress.auth import get_base_url, get_headers

log = get_logger(__name__)

_DS = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
_REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"
_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

SYSTEM_PROMPT = """\
You are an SEO copywriter for Pickleball Effect (pickleballeffect.com), run by \
Braydon Unsicker. Your job is to write compelling meta titles and descriptions \
that increase click-through rate from Google search results.

Rules — MUST follow every one:
- Title: 50-60 characters max (count carefully). Include the target keyword naturally.
- Description: 140-155 characters max. Write for the searcher, not for robots.
- Voice: Direct, confident, no hype. Braydon's voice — "court-tested", "no fluff",
  data-backed claims. Never use: "game-changer", "revolutionary", "amazing", "incredible".
- Include "2026" in titles for list/review content to signal freshness.
- Description should answer: what will I learn/get from clicking this?
- For reviews: hint at the verdict angle. For lists: hint at the ranking criteria.
- No exclamation marks. No ALL CAPS. No "Click here".

Output format — return ONLY a JSON array, one object per page, in the same order
as the input. Each object: {"id": <wp_id>, "type": "<posts|pages>",
"proposed_title": "...", "proposed_description": "..."}
No markdown, no explanation — just the raw JSON array.
"""


# ── WordPress helpers ──────────────────────────────────────────────────────────

def _fetch_wp_content() -> list[dict]:
    """Pull all published posts and pages with their current Yoast meta."""
    base_url = get_base_url()
    headers = get_headers()
    items = []

    for post_type in ("posts", "pages"):
        page = 1
        while True:
            resp = requests.get(
                f"{base_url}/{post_type}",
                headers=headers,
                params={
                    "status": "publish",
                    "per_page": 100,
                    "page": page,
                    "context": "edit",
                    "_fields": "id,slug,link,title,meta,yoast_head_json",
                },
                timeout=30,
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break

            for p in batch:
                meta = p.get("meta", {})
                yoast = p.get("yoast_head_json", {})
                # Use custom Yoast title if set, else fall back to rendered head title
                current_title = (
                    meta.get("_yoast_wpseo_title") or yoast.get("title", "")
                ).strip()
                current_desc = (
                    meta.get("_yoast_wpseo_metadesc") or yoast.get("description", "")
                ).strip()

                items.append({
                    "wp_id": p["id"],
                    "wp_type": post_type,
                    "slug": p.get("slug", ""),
                    "url": p.get("link", ""),
                    "post_title": p["title"]["rendered"],
                    "current_title": current_title,
                    "current_desc": current_desc,
                    "has_custom_yoast": bool(meta.get("_yoast_wpseo_title")),
                })

            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)

    log.info(f"Fetched {len(items)} published WP items")
    return items


# ── BigQuery helpers ───────────────────────────────────────────────────────────

def _fetch_sc_data() -> dict[str, dict]:
    """Return Search Console metrics keyed by page URL."""
    sql = f"""
    WITH page_metrics AS (
        SELECT
            page,
            SUM(impressions) AS impressions_30d,
            SUM(clicks)      AS clicks_30d,
            ROUND(AVG(position), 1) AS avg_position,
            ROUND(SAFE_DIVIDE(SUM(clicks), SUM(impressions)), 4) AS ctr
        FROM `{_DS}.search_console_performance`
        WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND site NOT LIKE '%shop%'
        GROUP BY page
        HAVING SUM(impressions) >= 200
    ),
    top_queries AS (
        SELECT
            page,
            ARRAY_AGG(query ORDER BY impressions DESC LIMIT 3) AS top_queries
        FROM `{_DS}.search_console_performance`
        WHERE query_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND site NOT LIKE '%shop%'
        GROUP BY page
    ),
    benchmarks AS (
        SELECT
            CAST(ROUND(avg_position) AS INT64) AS pos_bucket,
            AVG(ctr) AS benchmark_ctr
        FROM `{_DS}.vw_seo_opportunities`
        GROUP BY pos_bucket
    )
    SELECT
        m.page,
        m.impressions_30d,
        m.clicks_30d,
        m.avg_position,
        m.ctr,
        COALESCE(b.benchmark_ctr, 0.01) AS benchmark_ctr,
        ROUND((COALESCE(b.benchmark_ctr, 0.01) - m.ctr) * m.impressions_30d) AS missed_clicks,
        q.top_queries
    FROM page_metrics m
    LEFT JOIN benchmarks b ON CAST(ROUND(m.avg_position) AS INT64) = b.pos_bucket
    LEFT JOIN top_queries q USING (page)
    WHERE COALESCE(b.benchmark_ctr, 0.01) > m.ctr
    ORDER BY missed_clicks DESC
    """
    try:
        rows = list(run_query(sql))
    except Exception:
        log.exception("Failed to fetch Search Console data from BigQuery")
        return {}

    result = {}
    for r in rows:
        result[r.page] = {
            "impressions_30d": r.impressions_30d,
            "clicks_30d": r.clicks_30d,
            "avg_position": r.avg_position,
            "ctr": r.ctr,
            "benchmark_ctr": r.benchmark_ctr,
            "missed_clicks": int(r.missed_clicks or 0),
            "top_queries": list(r.top_queries or []),
        }

    log.info(f"Found {len(result)} pages with CTR gap in Search Console")
    return result


# ── Claude generation ──────────────────────────────────────────────────────────

def _generate_proposals(batch: list[dict]) -> list[dict]:
    """Send a batch of pages to Claude and return proposed title/desc per page."""
    lines = []
    for item in batch:
        sc = item["sc"]
        queries_str = ", ".join(f'"{q}"' for q in sc["top_queries"][:3])
        lines.append(
            f'- wp_id={item["wp_id"]} type={item["wp_type"]}\n'
            f'  post_title: {item["post_title"]}\n'
            f'  current_title ({len(item["current_title"])} chars): {item["current_title"]}\n'
            f'  current_desc ({len(item["current_desc"])} chars): {item["current_desc"]}\n'
            f'  top queries: {queries_str}\n'
            f'  position: {sc["avg_position"]} | impressions: {sc["impressions_30d"]:,} | '
            f'CTR: {sc["ctr"]:.2%} (benchmark: {sc["benchmark_ctr"]:.2%})\n'
            f'  url: {item["url"]}'
        )

    data_context = "\n\n".join(lines)
    question = (
        f"Write optimized Yoast SEO titles and meta descriptions for these "
        f"{len(batch)} pages. Return ONLY the JSON array as specified."
    )

    raw = analyze(SYSTEM_PROMPT, data_context, question)

    # Parse JSON from response (strip any accidental markdown fences)
    json_str = re.sub(r"^```[a-z]*\n?|```$", "", raw.strip(), flags=re.MULTILINE).strip()
    try:
        proposals = json.loads(json_str)
    except json.JSONDecodeError:
        log.warning(f"Failed to parse Claude response as JSON:\n{raw[:500]}")
        return []

    return proposals


# ── Proposal file helpers ──────────────────────────────────────────────────────

def _save_proposals(rows: list[dict], to_stdout: bool = False) -> Path | None:
    """Write the proposal review file."""
    today = date.today().strftime("%Y%m%d")
    filename = f"meta_optimization_{today}.md"

    total_missed = sum(r["sc"]["missed_clicks"] for r in rows)

    lines = [
        f"# SEO Meta Optimization Proposals — {date.today()}",
        f"**Pages with proposals:** {len(rows)}  ",
        f"**Estimated monthly clicks recoverable:** ~{total_missed:,}  ",
        f"**Status:** Pending Braydon review",
        "",
        "## How to approve",
        "- Review each proposal below",
        "- Edit proposed title/description directly in this file if needed",
        "- Delete any row you want to skip",
        "- Run: `uv run python -m seo.run --apply-meta-proposals` to push approved changes",
        "",
        "---",
        "",
        "<!-- PROPOSALS_START -->",
    ]

    for r in rows:
        sc = r["sc"]
        prop = r.get("proposal", {})
        lines += [
            f"## {r['post_title'][:70]}",
            f"**URL:** {r['url']}  ",
            f"**WP ID:** {r['wp_id']} | **Type:** {r['wp_type']}  ",
            f"**Position:** {sc['avg_position']} | "
            f"**Impressions:** {sc['impressions_30d']:,} | "
            f"**CTR:** {sc['ctr']:.2%} (benchmark: {sc['benchmark_ctr']:.2%}) | "
            f"**Missed clicks/mo:** ~{sc.get('missed_clicks', 0):,}  ",
            f"**Top queries:** {', '.join(sc['top_queries'][:3])}",
            "",
            f"**Current title** ({len(r['current_title'])} chars):  ",
            f"`{r['current_title']}`",
            "",
            f"**Proposed title** ({len(prop.get('proposed_title', ''))} chars):  ",
            f"`{prop.get('proposed_title', '(generation failed)')}`",
            "",
            f"**Current description** ({len(r['current_desc'])} chars):  ",
            f"`{r['current_desc']}`",
            "",
            f"**Proposed description** ({len(prop.get('proposed_description', ''))} chars):  ",
            f"`{prop.get('proposed_description', '(generation failed)')}`",
            "",
            "---",
            "",
        ]

    lines.append("<!-- PROPOSALS_END -->")
    content = "\n".join(lines)

    if to_stdout:
        print(content)
        return None

    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = _REPORTS_DIR / filename
    path.write_text(content, encoding="utf-8")
    log.info(f"Proposals saved to {path}")
    return path


def _load_proposals(path: Path) -> list[dict]:
    """Parse approved proposals from the review markdown file."""
    text = path.read_text(encoding="utf-8")

    # Extract each ## section
    sections = re.split(r"\n## ", text)
    proposals = []

    for section in sections[1:]:  # skip header
        wp_id_m = re.search(r"\*\*WP ID:\*\* (\d+)", section)
        wp_type_m = re.search(r"\*\*Type:\*\* (\w+)", section)
        title_m = re.search(
            r"\*\*Proposed title\*\*.*?\n`([^`]*)`", section, re.DOTALL
        )
        desc_m = re.search(
            r"\*\*Proposed description\*\*.*?\n`([^`]*)`", section, re.DOTALL
        )

        if not (wp_id_m and wp_type_m and title_m and desc_m):
            continue

        proposed_title = title_m.group(1).strip()
        proposed_desc = desc_m.group(1).strip()

        # Skip rows where generation failed
        if "(generation failed)" in proposed_title or not proposed_title:
            continue

        proposals.append({
            "wp_id": int(wp_id_m.group(1)),
            "wp_type": wp_type_m.group(1),
            "proposed_title": proposed_title,
            "proposed_desc": proposed_desc,
        })

    return proposals


# ── Public API ─────────────────────────────────────────────────────────────────

def propose(to_stdout: bool = False) -> Path | None:
    """Generate meta optimization proposals for all underperforming pages.

    Returns:
        Path to the saved proposals file, or None if to_stdout=True.
    """
    log.info("Fetching WordPress content inventory...")
    wp_items = _fetch_wp_content()

    log.info("Fetching Search Console performance data...")
    sc_data = _fetch_sc_data()

    # Join WP items with SC data
    candidates = []
    for item in wp_items:
        url = item["url"].rstrip("/") + "/"
        # Try both with and without trailing slash
        sc = sc_data.get(url) or sc_data.get(url.rstrip("/"))
        if sc:
            item["sc"] = sc
            candidates.append(item)

    # Sort by missed clicks descending
    candidates.sort(key=lambda x: x["sc"]["missed_clicks"], reverse=True)
    log.info(f"{len(candidates)} pages matched with Search Console data")

    if not candidates:
        log.warning("No pages with CTR gap found — nothing to optimize")
        return None

    # Generate proposals in batches of 5
    batch_size = 5
    all_proposal_map: dict[int, dict] = {}

    for i in range(0, len(candidates), batch_size):
        batch = candidates[i: i + batch_size]
        log.info(
            f"Generating proposals for batch {i // batch_size + 1}/"
            f"{(len(candidates) + batch_size - 1) // batch_size} "
            f"({len(batch)} pages)..."
        )
        proposals = _generate_proposals(batch)
        for prop in proposals:
            all_proposal_map[int(prop["id"])] = prop
        time.sleep(1)  # brief pause between Claude calls

    # Merge proposals back into candidates
    rows = []
    for item in candidates:
        item["proposal"] = all_proposal_map.get(item["wp_id"], {})
        rows.append(item)

    return _save_proposals(rows, to_stdout=to_stdout)


def apply(proposals_path: Path | None = None) -> int:
    """Push approved proposals from the review file to WordPress.

    Args:
        proposals_path: Path to the proposals markdown file. If None, uses
                        the most recent meta_optimization_*.md in reports/.

    Returns:
        Number of posts successfully updated.
    """
    if proposals_path is None:
        files = sorted(_REPORTS_DIR.glob("meta_optimization_*.md"), reverse=True)
        if not files:
            log.error("No meta optimization proposals file found in reports/")
            return 0
        proposals_path = files[0]

    log.info(f"Loading proposals from {proposals_path}")
    proposals = _load_proposals(proposals_path)

    if not proposals:
        log.warning("No valid proposals found in file")
        return 0

    log.info(f"Applying {len(proposals)} proposals to WordPress...")
    base_url = get_base_url()
    headers = get_headers()
    updated = 0

    for p in proposals:
        try:
            resp = requests.post(
                f"{base_url}/{p['wp_type']}/{p['wp_id']}",
                headers=headers,
                json={"meta": {
                    "_yoast_wpseo_title": p["proposed_title"],
                    "_yoast_wpseo_metadesc": p["proposed_desc"],
                }},
                timeout=30,
            )
            resp.raise_for_status()
            updated += 1
            log.info(f"  Updated {p['wp_type']} ID {p['wp_id']}: {p['proposed_title'][:50]}")
            time.sleep(1)
        except Exception as e:
            log.error(f"  Failed {p['wp_type']} ID {p['wp_id']}: {e}")

    log.info(f"Done — {updated}/{len(proposals)} proposals applied")
    return updated
