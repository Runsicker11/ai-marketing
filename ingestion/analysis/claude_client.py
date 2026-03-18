"""Thin wrapper around Claude API for marketing analysis."""

from pathlib import Path

import anthropic
import yaml

from ingestion.utils.config import ANTHROPIC_API_KEY
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_BRAND_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "brand.yaml"
_DEFAULT_MODEL = "claude-haiku-4-5-20251001"
_MAX_TOKENS = 8192


def _load_brand_context() -> str:
    """Load brand.yaml and format as context for Claude."""
    if not _BRAND_CONFIG_PATH.exists():
        return ""
    brand = yaml.safe_load(_BRAND_CONFIG_PATH.read_text(encoding="utf-8"))
    parts = [
        f"Brand: {brand.get('brand_name', '')}",
        f"Industry: {brand.get('industry', '')}",
        f"Sites: {brand.get('websites', {}).get('main', '')} (reviews), "
        f"{brand.get('websites', {}).get('shop', '')} (shop)",
    ]
    voice = brand.get("voice", {})
    if voice:
        parts.append(f"Tone: {voice.get('tone', '')}")
    offerings = brand.get("offerings", [])
    if offerings:
        products = ", ".join(o.get("name", "") for o in offerings)
        parts.append(f"Products: {products}")
    competitors = brand.get("competitors", [])
    if competitors:
        comp_names = ", ".join(c.get("name", "") for c in competitors)
        parts.append(f"Competitors: {comp_names}")
    return "\n".join(parts)


def analyze(system_prompt: str, data_context: str, question: str,
            model: str | None = None) -> str:
    """Send data + question to Claude and return the analysis.

    Args:
        system_prompt: Role/instructions for Claude.
        data_context: Formatted data tables/metrics to analyze.
        question: Specific question or task for Claude.
        model: Model ID override. Defaults to Haiku 4.5.
               Use "claude-sonnet-4-5-20250929" for structured JSON extraction.

    Returns:
        Claude's response text.
    """
    use_model = model or _DEFAULT_MODEL
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    brand_ctx = _load_brand_context()

    full_system = (
        f"{system_prompt}\n\n"
        f"## Brand Context\n{brand_ctx}"
    )

    user_message = (
        f"## Data\n\n{data_context}\n\n"
        f"## Task\n\n{question}"
    )

    log.info(f"Sending analysis request to {use_model} "
             f"({len(user_message)} chars of context)")

    response = client.messages.create(
        model=use_model,
        max_tokens=_MAX_TOKENS,
        system=full_system,
        messages=[{"role": "user", "content": user_message}],
    )

    text = response.content[0].text
    log.info(f"Received {len(text)} chars response "
             f"(tokens: {response.usage.input_tokens} in / "
             f"{response.usage.output_tokens} out)")
    return text
