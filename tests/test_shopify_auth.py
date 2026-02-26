"""Test Shopify Client Credentials Grant token acquisition."""

from ingestion.shopify.auth import get_access_token


def test_acquire_token():
    """Acquire a token and verify it's a non-empty string."""
    token = get_access_token()
    assert isinstance(token, str)
    assert len(token) > 10
    print(f"Token acquired: {token[:8]}...")


if __name__ == "__main__":
    test_acquire_token()
    print("Shopify auth test passed!")
