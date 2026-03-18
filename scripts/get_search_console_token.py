"""One-time script to get a Google Search Console OAuth2 refresh token.

Usage:
    uv run python scripts/get_search_console_token.py

This will:
1. Open your browser to Google's consent screen
2. You authorize the app
3. It prints the refresh token to paste into your .env
"""

import json
from google_auth_oauthlib.flow import InstalledAppFlow

# You can paste your client ID and secret here, or it will read from .env
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)

CLIENT_ID = os.environ.get("GOOGLE_SEARCH_CONSOLE_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET", "")

if not CLIENT_ID or not CLIENT_SECRET:
    print("ERROR: Set GOOGLE_SEARCH_CONSOLE_CLIENT_ID and GOOGLE_SEARCH_CONSOLE_CLIENT_SECRET in .env first")
    raise SystemExit(1)

# Build the OAuth config inline (no need for a downloaded JSON file)
client_config = {
    "installed": {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost"],
    }
}

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]

flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)
creds = flow.run_local_server(port=8085, prompt="consent", access_type="offline")

print("\n" + "=" * 60)
print("SUCCESS! Add this to your .env file:")
print("=" * 60)
print(f"\nGOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN={creds.refresh_token}\n")
