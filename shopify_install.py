"""
Shopify App Installation Helper

Spins up a local server, opens the Shopify OAuth approval page in your browser,
captures the authorization code, exchanges it for an access token, and tests it.

Usage: uv run python shopify_install.py
"""

import http.server
import threading
import webbrowser
import urllib.parse
import hashlib
import hmac
import requests
import sys
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env", override=True)

SHOP_DOMAIN = os.environ["SHOPIFY_SHOP_DOMAIN"]
CLIENT_ID = os.environ["SHOPIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SHOPIFY_CLIENT_SECRET"]

# Scopes we need for the marketing pipeline
SCOPES = "read_orders,read_products,read_customers"
REDIRECT_URI = "http://localhost:19456/callback"
PORT = 19456

# Will be filled by the callback handler
result = {"token": None, "error": None}
server_ref = {"server": None}


class CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/callback":
            params = urllib.parse.parse_qs(parsed.query)

            # Check for errors
            if "error" in params:
                result["error"] = params.get("error_description", params["error"])[0]
                self._respond("Installation failed. Check the terminal for details. You can close this tab.")
                threading.Thread(target=self._shutdown).start()
                return

            code = params.get("code", [None])[0]
            shop = params.get("shop", [None])[0]
            provided_hmac = params.get("hmac", [None])[0]

            if not code or not shop:
                result["error"] = f"Missing code or shop in callback. Params: {params}"
                self._respond("Missing parameters. Check terminal.")
                threading.Thread(target=self._shutdown).start()
                return

            # Verify HMAC
            if provided_hmac:
                query_params = {k: v[0] for k, v in params.items() if k != "hmac"}
                sorted_params = "&".join(f"{k}={v}" for k, v in sorted(query_params.items()))
                computed = hmac.new(
                    CLIENT_SECRET.encode(), sorted_params.encode(), hashlib.sha256
                ).hexdigest()
                if not hmac.compare_digest(computed, provided_hmac):
                    result["error"] = "HMAC verification failed"
                    self._respond("Security check failed. Check terminal.")
                    threading.Thread(target=self._shutdown).start()
                    return

            # Exchange code for access token
            print(f"\nGot authorization code. Exchanging for access token...")
            token_url = f"https://{shop}/admin/oauth/access_token"
            payload = {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "code": code,
            }
            resp = requests.post(token_url, json=payload, timeout=30)

            if resp.status_code == 200:
                data = resp.json()
                result["token"] = data.get("access_token")
                scopes = data.get("scope", "")
                self._respond(
                    f"Success! Token acquired. Scopes: {scopes}. You can close this tab."
                )
            else:
                result["error"] = f"Token exchange failed: {resp.status_code} {resp.text}"
                self._respond("Token exchange failed. Check terminal.")

            threading.Thread(target=self._shutdown).start()
        else:
            self.send_response(404)
            self.end_headers()

    def _respond(self, message):
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        html = f"<html><body style='font-family:sans-serif;padding:40px;text-align:center'><h2>{message}</h2></body></html>"
        self.wfile.write(html.encode())

    def _shutdown(self):
        server_ref["server"].shutdown()

    def log_message(self, format, *args):
        pass  # Suppress request logs


def main():
    # Build the install URL
    install_url = (
        f"https://{SHOP_DOMAIN}/admin/oauth/authorize"
        f"?client_id={CLIENT_ID}"
        f"&scope={SCOPES}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
    )

    print("=" * 60)
    print("Shopify App Installation Helper")
    print("=" * 60)
    print(f"\nShop:   {SHOP_DOMAIN}")
    print(f"Scopes: {SCOPES}")
    print(f"\nOpening your browser to authorize the app...")
    print(f"\nIf it doesn't open, manually visit:")
    print(f"\n  {install_url}\n")

    # Start local server
    server = http.server.HTTPServer(("localhost", PORT), CallbackHandler)
    server_ref["server"] = server

    # Open browser
    webbrowser.open(install_url)

    # Wait for callback
    print("Waiting for authorization callback...")
    server.serve_forever()

    if result["error"]:
        print(f"\nERROR: {result['error']}")
        sys.exit(1)

    token = result["token"]
    print(f"\nAccess token: {token[:12]}...{token[-4:]}")

    # Test the token
    print("\nTesting token...")
    test_url = f"https://{SHOP_DOMAIN}/admin/api/2024-10/shop.json"
    resp = requests.get(test_url, headers={"X-Shopify-Access-Token": token}, timeout=30)
    if resp.status_code == 200:
        shop_info = resp.json().get("shop", {})
        print(f"  Shop name: {shop_info.get('name')}")
        print(f"  Domain: {shop_info.get('domain')}")
        print(f"  Plan: {shop_info.get('plan_name')}")
        print(f"\n  TOKEN WORKS!")
    else:
        print(f"  Warning: test returned {resp.status_code}")

    # Offer to save
    print(f"\n{'=' * 60}")
    print("Add this to your .env file as SHOPIFY_ACCESS_TOKEN:")
    print(f"\nSHOPIFY_ACCESS_TOKEN={token}")
    print(f"\n{'=' * 60}")

    save = input("\nSave to .env automatically? [Y/n]: ").strip().lower()
    if save in ("", "y", "yes"):
        env_path = Path(__file__).parent / ".env"
        content = env_path.read_text()
        content = content.replace("SHOPIFY_ACCESS_TOKEN=", f"SHOPIFY_ACCESS_TOKEN={token}")
        env_path.write_text(content)
        print("Saved to .env!")
    else:
        print("Not saved. Copy the token above manually.")


if __name__ == "__main__":
    main()
