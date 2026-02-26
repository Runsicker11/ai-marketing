# Credential Setup Guide

Copy `.env.template` to `.env` first, then fill in values as you complete each section below.

```
cp .env.template .env
```

The `.env` file is gitignored and will never be committed.

---

## 1. Google Cloud Project (Do This First)

Everything runs on GCP, so set this up first.

### If you already have a project:
1. Go to [GCP Console](https://console.cloud.google.com)
2. Select your project from the dropdown at the top
3. Copy the **Project ID** (not the project name) → paste into `.env` as `GCP_PROJECT_ID`

### Enable required APIs:
1. Go to [APIs & Services > Library](https://console.cloud.google.com/apis/library)
2. Search for and enable each of these:
   - **BigQuery API** (should already be enabled)
   - **BigQuery Data Transfer API**
   - **Cloud Functions API**
   - **Cloud Scheduler API**
   - **Secret Manager API**
   - **Cloud Build API** (needed to deploy Cloud Functions)

### Create the BigQuery dataset:
1. Go to [BigQuery Console](https://console.cloud.google.com/bigquery)
2. Click the three dots next to your project name → **Create dataset**
3. Dataset ID: `marketing_data`
4. Location: `US`
5. Click **Create dataset**

**Add to `.env`:**
```
GCP_PROJECT_ID=your-project-id
GCP_REGION=us-central1
```

---

## 2. Google Ads API Credentials

This has the most steps but is a one-time setup.

### Step 1: Get a Developer Token
1. Sign in to [Google Ads](https://ads.google.com)
2. Click the **Tools** icon (wrench) → **Setup** → **API Center**
   - If you don't see API Center, you may need to go to the MCC (Manager) account
3. If you haven't applied for API access before, you'll see a form to fill out
4. Apply for **Basic Access** (this is sufficient for reading data)
5. Your developer token will show on this page once approved
   - Note: There's a "Test Account" token you can use immediately for testing
6. Copy the developer token → `.env` as `GOOGLE_ADS_DEVELOPER_TOKEN`

### Step 2: Create OAuth Credentials
1. Go to [GCP Console > APIs & Services > Credentials](https://console.cloud.google.com/apis/credentials)
2. Click **+ Create Credentials** → **OAuth client ID**
3. If prompted, configure the OAuth consent screen first:
   - User Type: **External** (or Internal if using Google Workspace)
   - App name: "Pickleball Effect Marketing"
   - User support email: your email
   - Developer contact email: your email
   - Click **Save and Continue** through the Scopes and Test Users sections
4. Back on Credentials page, click **+ Create Credentials** → **OAuth client ID**
5. Application type: **Desktop app**
6. Name: "AI Marketing"
7. Click **Create**
8. Copy **Client ID** → `.env` as `GOOGLE_ADS_CLIENT_ID`
9. Copy **Client Secret** → `.env` as `GOOGLE_ADS_CLIENT_SECRET`

### Step 3: Generate a Refresh Token
1. Open this URL in your browser (replace YOUR_CLIENT_ID):
```
https://accounts.google.com/o/oauth2/auth?client_id=YOUR_CLIENT_ID&redirect_uri=urn:ietf:wg:oauth:2.0:oob&scope=https://www.googleapis.com/auth/adwords&response_type=code&access_type=offline&prompt=consent
```
2. Sign in with the Google account that has access to your Google Ads
3. Click **Allow**
4. You'll get an **authorization code** — copy it
5. Exchange it for a refresh token by running this in your terminal (PowerShell):
```powershell
$body = @{
    code = "YOUR_AUTH_CODE"
    client_id = "YOUR_CLIENT_ID"
    client_secret = "YOUR_CLIENT_SECRET"
    redirect_uri = "urn:ietf:wg:oauth:2.0:oob"
    grant_type = "authorization_code"
}
Invoke-RestMethod -Uri "https://oauth2.googleapis.com/token" -Method POST -Body $body
```
6. Copy the `refresh_token` from the response → `.env` as `GOOGLE_ADS_REFRESH_TOKEN`

### Step 4: Get your Customer ID
1. Sign in to [Google Ads](https://ads.google.com)
2. Your Customer ID is in the top right, format: `123-456-7890`
3. Copy it → `.env` as `GOOGLE_ADS_CUSTOMER_ID` (keep the dashes or remove them, we'll handle both)
4. If you use a Manager (MCC) account, also copy that ID → `GOOGLE_ADS_LOGIN_CUSTOMER_ID`

**Add to `.env`:**
```
GOOGLE_ADS_CUSTOMER_ID=123-456-7890
GOOGLE_ADS_DEVELOPER_TOKEN=your-dev-token
GOOGLE_ADS_CLIENT_ID=your-client-id.apps.googleusercontent.com
GOOGLE_ADS_CLIENT_SECRET=your-client-secret
GOOGLE_ADS_REFRESH_TOKEN=your-refresh-token
```

---

## 3. Meta (Facebook) Ads API Credentials

### Step 1: Create a Meta App
1. Go to [Meta for Developers](https://developers.facebook.com/)
2. Click **My Apps** (top right) → **Create App**
3. Use case: **Other** → **Business** type
4. App name: "Pickleball Effect Marketing"
5. Business portfolio: select your business (or create one)
6. Click **Create App**
7. From the App Dashboard, copy:
   - **App ID** → `.env` as `META_APP_ID`
   - **App Secret** (click Show) → `.env` as `META_APP_SECRET`

### Step 2: Add Marketing API
1. In your app dashboard, click **Add Product** in the left sidebar
2. Find **Marketing API** and click **Set Up**

### Step 3: Get Your Access Token
**Quick method (short-lived token for testing):**
1. Go to [Graph API Explorer](https://developers.facebook.com/tools/explorer/)
2. Select your app from the dropdown
3. Click **Generate Access Token**
4. Grant permissions: `ads_read`, `ads_management`, `read_insights`
5. Copy the token → `.env` as `META_ACCESS_TOKEN`
   - Note: This token expires in ~1 hour. Fine for testing.

**Long-lived token (for production):**
1. Exchange the short-lived token by running in PowerShell:
```powershell
$url = "https://graph.facebook.com/v22.0/oauth/access_token?grant_type=fb_exchange_token&client_id=YOUR_APP_ID&client_secret=YOUR_APP_SECRET&fb_exchange_token=YOUR_SHORT_TOKEN"
Invoke-RestMethod -Uri $url
```
2. This gives you a token that lasts ~60 days
3. For a permanent token, you'll need a System User (we'll set this up later)

### Step 4: Get Your Ad Account ID
1. Go to [Meta Business Suite](https://business.facebook.com/)
2. Go to **Settings** → **Ad Accounts**
3. Your Ad Account ID looks like `act_123456789`
4. Copy it → `.env` as `META_ADS_ACCOUNT_ID`

**Add to `.env`:**
```
META_ADS_ACCOUNT_ID=act_123456789
META_APP_ID=your-app-id
META_APP_SECRET=your-app-secret
META_ACCESS_TOKEN=your-access-token
```

---

## 4. Shopify Admin API Credentials

### Step 1: Create a Custom App
1. Log in to [Shopify Admin](https://pickleballeffectshop.myshopify.com/admin)
2. Go to **Settings** (bottom left) → **Apps and sales channels**
3. Click **Develop apps** (top of page)
   - If you see "Allow custom app development", click it and confirm
4. Click **Create an app**
5. App name: "AI Marketing Pipeline"
6. Click **Create app**

### Step 2: Configure Permissions
1. Click **Configure Admin API scopes**
2. Select these scopes (read-only is sufficient):
   - `read_orders` — order data, revenue, line items
   - `read_products` — product names, prices, variants, inventory
   - `read_customers` — customer data for LTV analysis
   - `read_analytics` — store analytics
   - `read_reports` — sales reports
3. Click **Save**

### Step 3: Install and Get Token
1. Click **Install app** (top right)
2. Confirm the installation
3. You'll see **Admin API access token** — click **Reveal token once**
4. **IMPORTANT: Copy this immediately.** Shopify only shows it once!
5. Paste it → `.env` as `SHOPIFY_ACCESS_TOKEN`

**Add to `.env`:**
```
SHOPIFY_SHOP_DOMAIN=pickleballeffectshop.myshopify.com
SHOPIFY_ACCESS_TOKEN=shpat_xxxxxxxxxxxxxxxxxxxxx
```

---

## 5. Claude API Key

You likely already have this.

1. Go to [console.anthropic.com](https://console.anthropic.com)
2. Click **API Keys** in the left sidebar
3. Click **Create Key**
4. Name: "AI Marketing"
5. Copy the key → `.env` as `ANTHROPIC_API_KEY`

**Add to `.env`:**
```
ANTHROPIC_API_KEY=sk-ant-xxxxxxxxxxxxxxxxxxxxx
```

---

## Checklist

Once you've completed all sections, your `.env` should have all these filled in:

```
[ ] GCP_PROJECT_ID
[ ] GOOGLE_ADS_CUSTOMER_ID
[ ] GOOGLE_ADS_DEVELOPER_TOKEN
[ ] GOOGLE_ADS_CLIENT_ID
[ ] GOOGLE_ADS_CLIENT_SECRET
[ ] GOOGLE_ADS_REFRESH_TOKEN
[ ] META_ADS_ACCOUNT_ID
[ ] META_APP_ID
[ ] META_APP_SECRET
[ ] META_ACCESS_TOKEN
[ ] SHOPIFY_SHOP_DOMAIN
[ ] SHOPIFY_ACCESS_TOKEN
[ ] ANTHROPIC_API_KEY
```

When all 13 values are filled in, you're ready for Phase 1. Come back to the Claude Code session and say "credentials are ready" and we'll verify each connection and start building the pipeline.

---

## Security Notes

- **Never commit `.env`** — it's in `.gitignore` but double-check
- **Never share tokens in chat, email, or Slack**
- Once we're in production, all tokens move to **GCP Secret Manager**
- The `.env` file is only for local development and testing
- Meta tokens expire — we'll set up auto-refresh in the pipeline
