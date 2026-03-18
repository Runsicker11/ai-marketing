# Setup Guide: Two Things We Need Help With

## 1. WordPress Application Passwords (pickleballeffect.com)

**Problem:** Application Passwords are disabled by a security plugin (Wordfence). We need them enabled so our automation system can create draft blog posts via the WordPress API.

**Who can fix it:** Whoever has Super Admin access on the WordPress multisite network.

**Steps:**

1. Log in to WordPress at `https://pickleballeffect.com/wp-admin`
2. You need to be logged in as the **Super Admin** (network admin), not just a site admin
   - If you don't see **"My Sites" → "Network Admin"** in the top toolbar, you're not the Super Admin
   - The Super Admin is whoever originally installed WordPress
3. Go to **My Sites → Network Admin → Plugins** (or just go to **Wordfence → All Options** if Wordfence appears in your sidebar)
4. Search for **"Application Passwords"**
5. Uncheck **"Disable WordPress application passwords"**
6. Click **Save**

**After that's done, we do this part:**

7. Go to **Users → Profile** (your own profile)
8. Scroll down to **Application Passwords** section
9. Enter name: `ai-marketing`
10. Click **Add New Application Password**
11. Copy the password it shows (only shown once!) — looks like: `xxxx xxxx xxxx xxxx xxxx xxxx`

**What we need to write down:**
- WordPress username (the login username, not display name)
- The application password

---

## 2. Search Console Access for Review Site (pickleballeffect.com)

**Problem:** Our API credentials use the `support@pickleballeffect.com` Google account, but that account isn't added to the review site's Search Console property. The Shopify site (pickleballeffectshop.com) is already working.

**Who can fix it:** Whoever verified/owns the pickleballeffect.com property in Google Search Console.

**Steps:**

1. Go to [Google Search Console](https://search.google.com/search-console)
2. Log in with the account that owns the **pickleballeffect.com** property
3. Make sure you're viewing the **pickleballeffect.com** property (use the dropdown at the top-left)
4. Click **Settings** (gear icon in the left sidebar)
5. Click **Users and permissions**
6. Click **Add user**
7. Enter email: `support@pickleballeffect.com`
8. Set permission to: **Full**
9. Click **Add**

**That's it.** Nothing else to write down — the API credentials are already configured.

---

## Quick Verification

After both are done, we can verify immediately:

- **WordPress:** We'll run a script that calls the WordPress API and confirms access
- **Search Console:** We'll run a script that pulls search query data for pickleballeffect.com

Both take about 10 seconds to test.

---

## What We're Building: SEO Content Strategy

Once these two APIs are connected, here's the plan. The key principle: **nothing publishes without human review and Braydon's approval.** This is a tool that assists content creation, not one that runs on autopilot.

### The Vision

We have a significant competitive advantage: 100+ paddle reviews, 32K monthly visitors, 1.6M impressions, and strong Google organic traffic (69% of review site visitors come from Google). Meanwhile, our direct product competitors (Bodhi, UDrippin, Flick Weight) have essentially **zero** SEO content. We can own the informational search space for paddle accessories and customization topics that directly support our product sales and affiliate revenue.

### Two Revenue Streams, One Content Strategy

**Affiliate revenue (primary):** The review site drives people to read paddle reviews, use coupon codes (PBEFFECT), and click affiliate links to retailers. This is the main revenue engine. SEO content here focuses on review-intent and research-intent keywords — "best pickleball paddle 2026", "JOOLA Pro V review", comparison guides.

**Product sales (secondary but growing):** The shop sells accessories — tungsten tape, overgrips, edge guard tape. The review site already drives ~$1,200/month in shop revenue from organic cross-linking, but only 1.1% of review site visitors click through to the shop. There's room to grow this significantly with better contextual cross-linking.

### Content Types We'll Create

**For the review site (pickleballeffect.com):**
- Accessory guides — "Tungsten Tape vs Lead Tape: Complete Guide", "Best Pickleball Overgrips 2026"
- Paddle customization content — "How to Weight Your Pickleball Paddle", "Paddle Customization Guide"
- Expanded review content — deeper buying guides that connect paddle reviews to accessory recommendations
- These naturally link to both affiliate retailers AND the shop where relevant

**For the shop site (pickleballeffectshop.com):**
- Product-specific usage guides — "How to Apply Tungsten Tape in 3 Steps"
- Product comparison pages — our products vs alternatives
- Customer use cases and applications
- These build buying-intent SEO directly on the shop domain

### How It Works (Human-in-the-Loop)

**Phase 1: Research (automated)**
- Pull Search Console data to see what queries people search for
- Identify content gaps — queries with high impressions but low clicks, or topics competitors rank for that we don't
- Analyze which existing content drives the most affiliate clicks and shop revenue
- Generate a prioritized list of content opportunities

**Phase 2: Content brief (AI-assisted, human-approved)**
- For each approved topic, generate a content brief: target keywords, outline, angle, internal links
- Braydon reviews and adjusts — adds opinions, decides the angle, flags what to include/exclude
- No content gets written until the brief is approved

**Phase 3: Draft creation (AI-assisted, human-reviewed)**
- Generate a draft in Braydon's voice — direct, opinion-forward, data-informed, no fluff
- Include placeholder spots for relevant images from the photo library
- Flag where affiliate links, shop links, and coupon codes should go
- Draft is saved for review, NOT published

**Phase 4: Human review and polish (manual)**
- Braydon reviews every draft for voice, accuracy, and opinions
- Adds personal testing insights that AI can't know
- Selects and places actual images from the photo library
- Approves or revises before anything goes live

**Phase 5: Publishing (semi-automated)**
- Once approved, the system can push the draft to WordPress or Shopify as a draft post
- Final review in the actual CMS before hitting publish
- Track performance in Search Console and GA4 after publishing

### Images and Visual Content

We want to use Braydon's actual testing photos and product images to keep content authentic and on-brand. The plan:

- Braydon provides a library of images (product photos, testing shots, paddle close-ups, on-court photos)
- We tag/organize them by product, topic, and content type
- When generating drafts, the system suggests image placements (e.g., "INSERT: close-up of tungsten tape application" or "INSERT: paddle on scale showing weight")
- Braydon selects the actual images during review — the AI suggests placement, the human picks the photo

This keeps the visual quality high and authentic (real photos from real testing, not stock images), which matters for both SEO (Google favors original images) and brand trust.

### What We're NOT Doing

- No auto-publishing. Everything is a draft until a human approves it.
- No AI-generated opinions. Braydon's takes on paddles and products are his own — AI structures and writes, but the opinions come from him.
- No duplicate content across both sites. Each piece of content has a clear home (review site vs shop) with complementary angles.
- No generic SEO spam. Quality over quantity — 2 great articles per month beat 10 mediocre ones.

### Early Priority Content (Once APIs Are Connected)

Based on competitor gaps and search data, here's what we'd start with:

1. **"Tungsten Tape vs Lead Tape for Pickleball: Complete Guide"** — Our #1 product, and small competitors currently own this query with mediocre content. Should live on the review site with natural shop links.

2. **"Best Pickleball Overgrips 2026: Tested & Ranked"** — Head-to-head comparisons including competitor products and our own. Honest, data-backed. Review site.

3. **"How to Weight Your Pickleball Paddle"** — Flick Weight (our competitor) should own this but doesn't. Covers handle weights, edge tape, tungsten tape, and USAPA rules. Review site.

4. **"How to Apply Tungsten Tape: Step-by-Step Guide"** — Product-specific, buying-intent. Shop site blog.

5. **"Pickleball Paddle Customization Guide"** — Umbrella content covering everything you can legally modify. Becomes the definitive reference. Review site.

These would all go through the full review process above before anything gets published.

### Success Metrics

We'll track:
- Organic traffic growth (Search Console impressions and clicks)
- Affiliate click-through rate from content (once GA4 custom events are set up)
- Shop referral traffic and revenue from new content
- Keyword rankings for target queries
- Content engagement (time on page, scroll depth)

All of this feeds into the weekly strategy report so we can see what's working and adjust.
