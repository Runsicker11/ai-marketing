"""Unit tests for UTM parameter extraction from landing_site URLs."""

from ingestion.shopify.pull_orders import parse_utms


def test_full_utm_url():
    url = "https://pickleballeffectshop.com/products/paddle?utm_source=facebook&utm_medium=cpc&utm_campaign=summer_sale&utm_content=video_ad&utm_term=pickleball+paddles"
    result = parse_utms(url)
    assert result["utm_source"] == "facebook"
    assert result["utm_medium"] == "cpc"
    assert result["utm_campaign"] == "summer_sale"
    assert result["utm_content"] == "video_ad"
    assert result["utm_term"] == "pickleball paddles"


def test_partial_utm():
    url = "https://example.com/page?utm_source=google&utm_medium=organic"
    result = parse_utms(url)
    assert result["utm_source"] == "google"
    assert result["utm_medium"] == "organic"
    assert result["utm_campaign"] is None
    assert result["utm_content"] is None
    assert result["utm_term"] is None


def test_no_utms():
    url = "https://pickleballeffectshop.com/collections/all"
    result = parse_utms(url)
    assert all(v is None for v in result.values())


def test_none_input():
    result = parse_utms(None)
    assert all(v is None for v in result.values())


def test_empty_string():
    result = parse_utms("")
    assert all(v is None for v in result.values())


def test_malformed_url():
    result = parse_utms("not a url at all")
    assert all(v is None for v in result.values())


def test_relative_path_with_params():
    url = "/products/overgrip?utm_source=meta&utm_campaign=retarget"
    result = parse_utms(url)
    assert result["utm_source"] == "meta"
    assert result["utm_campaign"] == "retarget"


def test_encoded_characters():
    url = "https://shop.com/?utm_source=facebook&utm_campaign=spring%20sale"
    result = parse_utms(url)
    assert result["utm_source"] == "facebook"
    assert result["utm_campaign"] == "spring sale"
