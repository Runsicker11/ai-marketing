# GA4 Configuration Guide: Cross-Domain Tracking & Affiliate Click Events

## What We're Setting Up

| Change | What It Does |
|--------|-------------|
| Consolidate to one GA4 property | Both sites send data to one place so you can see the full user journey from review → shop → purchase |
| Cross-domain measurement | GA4 recognizes the same user across both domains (no more broken sessions) |
| `affiliate_click` event | Fires when someone clicks to joola.com, pickleballcentral.com, etc. — lets us track and optimize affiliate click-through rate |
| `shop_click` event | Fires when someone clicks to pickleballeffectshop.com — lets us measure review-to-shop conversion |
| Custom dimensions | Makes link URLs visible in reports so we can see exactly which links get clicked |

---

## IMPORTANT: You Need to Consolidate to ONE GA4 Property First

Cross-domain tracking requires both websites to send data to the **same GA4 property**. Right now you have two separate properties — one for the review site and one for the shop. GA4 can't stitch user sessions across two separate properties.

**Plan:** Use the review site's GA4 property (stream ID `2150543455`) as the master. Add the shop as a second data stream under that same property. The old shop property stays intact with its historical data — nothing gets deleted.

---

## Part 1: Cross-Domain Measurement Setup

### Step 1: Add the Shop as a Second Data Stream

1. Open [Google Analytics](https://analytics.google.com) and make sure you're in the **review site's GA4 property** (the one tracking pickleballeffect.com). Check the property name at the top of the left sidebar.

2. Click the **gear icon** (Admin) in the bottom-left corner.

3. In the Property column, click **Data Streams**.

4. You'll see your existing stream for pickleballeffect.com. Click the blue **Add stream** button in the top-right.

5. Select **Web**.

6. **Website URL:** enter `pickleballeffectshop.com`

7. **Stream name:** enter `Pickleball Effect Shop`

8. Leave **Enhanced measurement** toggled ON.

9. Click **Create stream**.

10. **Copy the new Measurement ID** (starts with `G-`). You'll need it in the next step.

### Step 2: Install the New Measurement ID on Shopify

You need to replace the old GA4 tracking on Shopify with the new measurement ID.

**Option A: Via Google & YouTube Sales Channel (Recommended)**

1. Log in to Shopify admin at `https://1cee15-1f.myshopify.com/admin`
2. Left sidebar → **Sales channels** → **Google & YouTube**
   - If you don't see this app, go to **Apps** → **Shopify App Store**, search "Google & YouTube," and install it
3. Find the **Google Analytics** section. If an old property is connected, click **Disconnect**
4. Click **Connect** next to Google Analytics
5. Select the **review site's property** (the master one with both streams) from the dropdown
6. Click **Connect**

**Option B: Manual via Shopify Preferences**

1. Shopify admin → **Online Store** → **Preferences**
2. Scroll to **Google Analytics**
3. Paste the new Measurement ID (`G-XXXXXXXXXX` from Step 1)
4. Click **Save**

**Important:** Make sure the OLD measurement ID is completely removed. Check **Online Store** → **Themes** → **Edit code** → `theme.liquid` for any leftover `gtag` or `G-` references. Having two GA4 tags causes double-counting.

### Step 3: Verify WordPress (Probably No Changes Needed)

Since the review site's property is staying as the master, your existing Site Kit setup should keep working.

To verify:
1. WordPress admin → **Site Kit** → **Settings**
2. Click the **Analytics** section
3. Confirm the property matches your master property
4. If it matches, no changes needed

### Step 4: Configure Cross-Domain Measurement

This tells GA4 to pass user identity between domains via a URL parameter.

1. In GA4, go to **Admin** (gear icon) → **Data Streams**

2. Click your **pickleballeffect.com** stream

3. Scroll down and click **Configure tag settings**

4. Click **Configure your domains**

5. Click **Add condition**:
   - **Match type:** `Contains`
   - **Domain:** `pickleballeffect.com`

6. Click **Add condition** again:
   - **Match type:** `Contains`
   - **Domain:** `pickleballeffectshop.com`

7. Click **Save**

8. **Repeat for the shop stream:** Go back to Data Streams → click the **pickleballeffectshop.com** stream → Configure tag settings → Configure your domains → add both domains the same way → Save

### Step 5: Test It

1. Install the **Google Analytics Debugger** Chrome extension (free). Click its icon to turn it ON.

2. In GA4, go to **Admin** → **DebugView**

3. Open a new tab, go to **pickleballeffect.com**, and click a link to **pickleballeffectshop.com**

4. Check the URL in your address bar after landing on the shop. You should see `_gl=` appended:
   ```
   https://pickleballeffectshop.com/?_gl=1*abc123*_ga*MTIz...
   ```

5. In DebugView, you should see `page_view` events from BOTH domains in the same session.

If you don't see the `_gl=` parameter, double-check that both domains are listed in Configure your domains for both streams.

---

## Part 2: Affiliate Link Click Tracking

This is the setup that lets us track how often people click affiliate links vs. shop links, which pages drive the most clicks, and optimize that click-through rate over time.

### Step 1: Create the "affiliate_click" Event

1. GA4 → **Admin** (gear icon)

2. Under Property, go to **Data display** → **Events**

3. Click **Create event** (top-right) → **Create**

4. **Custom event name:** `affiliate_click`

5. Set matching conditions:

   **Condition 1:**
   - **Parameter:** `event_name`
   - **Operator:** `equals`
   - **Value:** `click`

6. Click **Add condition**

   **Condition 2:**
   - **Parameter:** `link_url`
   - **Operator:** `matches RegEx (ignore case)`
   - **Value:** (paste all your affiliate domains separated by `|`)
     ```
     joola\.com|pickleballcentral\.com|selkirk\.com|crbnpickleball\.com|sixzeropickleball\.com|vaticpro\.com|engagepickleball\.com|franklinpickleball\.com|ronbus\.com|babolat\.com|headpickleball\.com
     ```
     Add or remove domains to match your actual affiliate partners. The `\.` before `com` is important — it means "literal dot."

7. Check **Copy parameters from the source event**

8. Click **Create**

**Simpler alternative if RegEx feels complicated:** Instead of one event with RegEx, create individual events for each partner using **Operator:** `contains` and **Value:** `joola.com` (one domain per event). You can have up to 50 custom events.

### Step 2: Create the "shop_click" Event

1. Still in **Events**, click **Create event** → **Create**

2. **Custom event name:** `shop_click`

3. Conditions:

   **Condition 1:**
   - **Parameter:** `event_name`
   - **Operator:** `equals`
   - **Value:** `click`

   **Condition 2:**
   - **Parameter:** `link_url`
   - **Operator:** `contains`
   - **Value:** `pickleballeffectshop.com`

4. Check **Copy parameters from the source event**

5. Click **Create**

### Step 3: Mark as Key Events (Do This After 24 Hours)

Custom events take up to 24 hours to appear in the events list after someone first triggers them.

1. Go to **Admin** → **Data display** → **Events**
2. Wait until `affiliate_click` and `shop_click` appear in the list
3. Find the **Mark as key event** toggle on the right side of each row
4. Flip it ON (blue) for both

This tells GA4 these are important actions — useful if you ever run Google Ads campaigns pointing to the review site.

### Step 4: Register link_url as a Custom Dimension

This makes the actual clicked URL visible in reports (not just the event count).

1. Go to **Admin** → **Data display** → **Custom definitions**

2. Click **Create custom dimension**:
   - **Dimension name:** `Link URL`
   - **Scope:** `Event`
   - **Event parameter:** `link_url`
   - Click **Save**

3. (Optional) Create another:
   - **Dimension name:** `Link Domain`
   - **Scope:** `Event`
   - **Event parameter:** `link_domain`
   - Click **Save**

### Step 5: Test Your Events

1. Turn on **Google Analytics Debugger** Chrome extension
2. Open **Admin** → **DebugView** in GA4
3. Go to pickleballeffect.com and click an affiliate link (e.g., a JOOLA buy link)
4. In DebugView, you should see a `click` event followed by an `affiliate_click` event
5. Click the event bubble to verify the `link_url` parameter shows the correct URL
6. Go back and click a link to pickleballeffectshop.com — verify you see a `shop_click` event

Events can take up to 1 hour to start firing after you first create them.

---

## Part 3: How We'll Use This Data

Once this is set up and collecting data (give it at least a week), we can:

1. **Measure affiliate click-through rate** — what % of review site visitors click an affiliate link? Which pages have the highest/lowest rates?
2. **Optimize click placement** — A/B test different CTA positions, button text, and link placement in reviews to increase the rate
3. **Track shop conversion funnel** — see the complete journey from "reads review" → "clicks to shop" → "adds to cart" → "purchases"
4. **Attribute revenue to content** — which specific review articles drive the most affiliate clicks AND the most shop purchases?
5. **Reduce the "Direct" mystery bucket** — cross-domain tracking will properly attribute shop visits that come from the review site instead of lumping them into Direct

---

## Maintaining Your Affiliate Domain List

When you add a new affiliate partner, update the `affiliate_click` event:

1. **Admin** → **Data display** → **Events**
2. Click on the `affiliate_click` event rule
3. Edit the RegEx value to add the new domain: `|newpartner\.com`
4. Click **Save**

---

## Notes

- Historical data is NOT affected — these changes only apply going forward
- Created events take up to 1 hour to start firing
- New events take up to 24 hours to appear in standard reports (DebugView is real-time)
- The old shop GA4 property retains all its data — nothing is deleted
- Your existing BigQuery export will need to be updated to point to the master property once consolidated (we can handle this together)
