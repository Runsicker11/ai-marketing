"""Test Meta Ads token validation."""

from ingestion.meta.auth import validate_token


def test_token_valid():
    """Validate the Meta access token and print debug info."""
    info = validate_token()
    assert info["is_valid"] is True
    print(f"Token valid, scopes: {info['scopes']}")
    if info["days_remaining"] is not None:
        print(f"Days remaining: {info['days_remaining']}")


if __name__ == "__main__":
    test_token_valid()
    print("Meta auth test passed!")
