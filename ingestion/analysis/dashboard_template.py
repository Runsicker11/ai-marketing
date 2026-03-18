"""HTML template for the monthly performance dashboard."""

from string import Template

DASHBOARD_HTML = Template("""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Monthly Performance Dashboard — $month_label</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/highcharts@11.4.1/highcharts.js"></script>
<script src="https://cdn.jsdelivr.net/npm/highcharts@11.4.1/modules/funnel.js"></script>
<script src="https://cdn.jsdelivr.net/npm/highcharts@11.4.1/modules/exporting.js"></script>
<script src="https://cdn.jsdelivr.net/npm/highcharts@11.4.1/modules/export-data.js"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }

body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    background: #f0f2f5;
    color: #1a1a2e;
    line-height: 1.6;
}

.header {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: #fff;
    padding: 40px 48px;
}
.header h1 {
    font-size: 32px;
    font-weight: 700;
    margin-bottom: 6px;
}
.header .subtitle {
    font-size: 16px;
    opacity: 0.85;
    font-weight: 400;
}

.container {
    max-width: 1280px;
    margin: 0 auto;
    padding: 32px 24px 48px;
}

/* -- KPI Cards -- */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(6, 1fr);
    gap: 18px;
    margin-bottom: 36px;
}
.kpi-card {
    background: #fff;
    border-radius: 12px;
    padding: 22px 20px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    transition: all 0.3s ease;
    position: relative;
}
.kpi-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 8px 24px rgba(0,0,0,0.1);
}
.kpi-label {
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: #6b7280;
    margin-bottom: 8px;
}
.kpi-info {
    display: inline-block;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    background: #e5e7eb;
    color: #6b7280;
    font-size: 10px;
    font-weight: 700;
    text-align: center;
    line-height: 14px;
    cursor: help;
    margin-left: 4px;
    vertical-align: middle;
    position: relative;
}
.kpi-info:hover { background: #d1d5db; }
.kpi-tooltip {
    display: none;
    position: absolute;
    bottom: calc(100% + 8px);
    left: 50%;
    transform: translateX(-50%);
    background: #1a1a2e;
    color: #fff;
    font-size: 12px;
    font-weight: 400;
    text-transform: none;
    letter-spacing: 0;
    line-height: 1.5;
    padding: 10px 14px;
    border-radius: 8px;
    width: 240px;
    z-index: 100;
    box-shadow: 0 4px 16px rgba(0,0,0,0.2);
    pointer-events: none;
}
.kpi-tooltip::after {
    content: '';
    position: absolute;
    top: 100%;
    left: 50%;
    transform: translateX(-50%);
    border: 6px solid transparent;
    border-top-color: #1a1a2e;
}
.kpi-info:hover .kpi-tooltip { display: block; }
.kpi-value {
    font-size: 28px;
    font-weight: 700;
    color: #1a1a2e;
    margin-bottom: 6px;
}
.kpi-delta {
    display: inline-block;
    font-size: 13px;
    font-weight: 600;
    padding: 3px 10px;
    border-radius: 20px;
}
.kpi-delta.positive { background: #d1fae5; color: #065f46; }
.kpi-delta.negative { background: #fee2e2; color: #991b1b; }
.kpi-delta.neutral  { background: #e5e7eb; color: #374151; }

/* -- Section cards -- */
.section {
    background: #fff;
    border-radius: 12px;
    padding: 28px;
    margin-bottom: 24px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
.section h2 {
    font-size: 20px;
    font-weight: 700;
    margin-bottom: 20px;
    color: #1a1a2e;
}
.section .note {
    font-size: 12px;
    color: #9ca3af;
    margin-top: -14px;
    margin-bottom: 16px;
}

.chart-container { width: 100%; min-height: 380px; }

/* -- Two-column layout -- */
.two-col {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
    margin-bottom: 24px;
}
@media (max-width: 900px) {
    .two-col { grid-template-columns: 1fr; }
}

/* -- Data Tables -- */
.data-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
}
.data-table th {
    text-align: left;
    padding: 10px 14px;
    border-bottom: 2px solid #e5e7eb;
    font-weight: 600;
    color: #6b7280;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
}
.data-table th.right, .data-table td.right {
    text-align: right;
}
.data-table td {
    padding: 10px 14px;
    border-bottom: 1px solid #f3f4f6;
}
.data-table tbody tr:hover {
    background: #f9fafb;
}
.data-table tfoot td {
    font-weight: 700;
    border-top: 2px solid #e5e7eb;
}

/* Legacy alias */
.health-table { width: 100%; border-collapse: collapse; font-size: 14px; }
.health-table th {
    text-align: left; padding: 10px 14px; border-bottom: 2px solid #e5e7eb;
    font-weight: 600; color: #6b7280; font-size: 12px;
    text-transform: uppercase; letter-spacing: 0.4px;
}
.health-table td { padding: 10px 14px; border-bottom: 1px solid #f3f4f6; }
.health-table tbody tr:hover { background: #f9fafb; }

.tier-badge {
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    padding: 2px 10px;
    border-radius: 12px;
    text-transform: capitalize;
}
.tier-top_performer { background: #d1fae5; color: #065f46; }
.tier-good          { background: #dbeafe; color: #1e40af; }
.tier-marginal      { background: #fef3c7; color: #92400e; }
.tier-underperformer { background: #fee2e2; color: #991b1b; }
.tier-wasted_spend  { background: #fce7f3; color: #9d174d; }

/* -- MoM change badges -- */
.mom-up   { color: #065f46; }
.mom-down { color: #991b1b; }
.mom-flat { color: #6b7280; }

/* -- P&L table -- */
.pnl-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    overflow-x: auto;
}
.pnl-table th, .pnl-table td {
    padding: 8px 10px;
    text-align: right;
    white-space: nowrap;
}
.pnl-table th:first-child, .pnl-table td:first-child {
    text-align: left;
    font-weight: 600;
}
.pnl-table thead th {
    border-bottom: 2px solid #e5e7eb;
    font-weight: 600;
    color: #6b7280;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
}
.pnl-table tbody td {
    border-bottom: 1px solid #f3f4f6;
}
.pnl-table tbody tr:hover { background: #f9fafb; }
.pnl-table tfoot td {
    font-weight: 700;
    border-top: 2px solid #e5e7eb;
}
.pnl-positive { color: #065f46; }
.pnl-negative { color: #991b1b; }

/* -- Footer -- */
.footer {
    text-align: center;
    padding: 24px;
    font-size: 12px;
    color: #9ca3af;
}

@media print {
    .section { page-break-inside: avoid; }
    .header { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
}
</style>
</head>
<body>

<!-- -- Header -- -->
<div class="header">
    <h1>Monthly Performance Dashboard</h1>
    <div class="subtitle">$month_label &nbsp;|&nbsp; Compared to $prior_month_label</div>
</div>

<div class="container">

<!-- -- KPI Cards -- -->
<div class="kpi-grid">
    <div class="kpi-card">
        <div class="kpi-label">Revenue <span class="kpi-info">?<span class="kpi-tooltip">Platform-reported revenue across all ad channels (Meta + Google Ads). Based on each platform's conversion tracking.</span></span></div>
        <div class="kpi-value">$kpi_revenue</div>
        <span class="kpi-delta $delta_revenue_class">$delta_revenue</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">AOV <span class="kpi-info">?<span class="kpi-tooltip">Average Order Value. Total platform-reported revenue divided by total orders. Higher AOV means more revenue per transaction.</span></span></div>
        <div class="kpi-value">$kpi_aov</div>
        <span class="kpi-delta $delta_aov_class">$delta_aov</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Contrib. Margin <span class="kpi-info">?<span class="kpi-tooltip">Contribution Margin per Order. After ALL costs (COGS, shipping, ad spend, agency fee), how much each order contributes. Negative = losing money per order.</span></span></div>
        <div class="kpi-value">$kpi_contrib_margin</div>
        <span class="kpi-delta $delta_contrib_margin_class">$delta_contrib_margin per order</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Orders <span class="kpi-info">?<span class="kpi-tooltip">Total orders reported by ad platforms (Meta + Google Ads). May differ from Shopify order count due to attribution differences.</span></span></div>
        <div class="kpi-value">$kpi_orders</div>
        <span class="kpi-delta $delta_orders_class">$delta_orders</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Blended ROAS <span class="kpi-info">?<span class="kpi-tooltip">Return on Ad Spend. Revenue / Ad Spend (ads only, excludes agency fee). A 3.0x ROAS means $$3 revenue for every $$1 spent on ads.</span></span></div>
        <div class="kpi-value">$kpi_roas</div>
        <span class="kpi-delta $delta_roas_class">$delta_roas</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">MER <span class="kpi-info">?<span class="kpi-tooltip">Marketing Efficiency Ratio. Revenue / Total Marketing Cost (ad spend + $$3K agency fee). The true efficiency metric &mdash; unlike ROAS, MER accounts for all marketing expenses. Benchmark: 3.0x.</span></span></div>
        <div class="kpi-value">$kpi_mer</div>
        <span class="kpi-delta $delta_mer_class">$delta_mer</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">CVR <span class="kpi-info">?<span class="kpi-tooltip">Conversion Rate. GA4 purchases / GA4 sessions. The percentage of website visitors who complete a purchase.</span></span></div>
        <div class="kpi-value">$kpi_cvr</div>
        <span class="kpi-delta $delta_cvr_class">$delta_cvr</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Customers <span class="kpi-info">?<span class="kpi-tooltip">Total unique Shopify customers who placed at least one order this month.</span></span></div>
        <div class="kpi-value">$kpi_total_customers</div>
        <span class="kpi-delta $delta_total_customers_class">$delta_total_customers</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">New Customers <span class="kpi-info">?<span class="kpi-tooltip">First-time buyers. Customers whose very first Shopify order was placed this month. Measures acquisition effectiveness.</span></span></div>
        <div class="kpi-value">$kpi_new_customers</div>
        <span class="kpi-delta $delta_new_customers_class">$delta_new_customers</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Returning <span class="kpi-info">?<span class="kpi-tooltip">Repeat buyers. Customers who ordered this month but whose first order was in a prior month. Measures retention and loyalty.</span></span></div>
        <div class="kpi-value">$kpi_returning_customers</div>
        <span class="kpi-delta $delta_returning_customers_class">$delta_returning_customers</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">CAC <span class="kpi-info">?<span class="kpi-tooltip">Customer Acquisition Cost. Ad Spend / New Customers. How much it costs to acquire one first-time buyer. Lower is better.</span></span></div>
        <div class="kpi-value">$kpi_cac</div>
        <span class="kpi-delta $delta_cac_class">$delta_cac</span>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Avg CLTV (6mo) <span class="kpi-info">?<span class="kpi-tooltip">Customer Lifetime Value (6-month). Average gross profit per customer over their first 6 months. Compare to CAC &mdash; CLTV should exceed CAC for sustainable growth.</span></span></div>
        <div class="kpi-value">$kpi_cltv</div>
        <span class="kpi-delta neutral">All cohorts</span>
    </div>
</div>

<!-- -- Monthly P&L (Trailing 13 Months) -- -->
<div class="section">
    <h2>Monthly P&amp;L</h2>
    <p class="note">Trailing 6 months. Gross Sales matches Shopify Analytics. Shipping cost = $$5.35/order. Agency = $$3,000/mo.</p>
    <div style="overflow-x: auto;">
        <table class="pnl-table" id="pnl-table">
            <thead id="pnl-thead"></thead>
            <tbody id="pnl-tbody"></tbody>
            <tfoot id="pnl-tfoot"></tfoot>
        </table>
    </div>
</div>

<!-- -- Channel KPIs Table -- -->
<div class="section">
    <h2>Channel KPIs</h2>
    <p class="note">Platform-reported metrics. Revenue = platform conversion value.</p>
    <div style="overflow-x: auto;">
        <table class="data-table" id="channel-kpis-table">
            <thead>
                <tr>
                    <th>Channel</th>
                    <th class="right">Spend</th>
                    <th class="right">Clicks</th>
                    <th class="right">CTR</th>
                    <th class="right">Conv.</th>
                    <th class="right">Revenue</th>
                    <th class="right">ROAS</th>
                    <th class="right">CPA</th>
                    <th class="right">AOV</th>
                </tr>
            </thead>
            <tbody id="channel-kpis-rows"></tbody>
        </table>
    </div>
</div>

<!-- -- Sessions & CVR + New Customers -- -->
<div class="two-col">
    <div class="section">
        <h2>Sessions &amp; CVR (GA4)</h2>
        <p class="note">GA4 sessions by source/medium. Google split by CPC / Product Sync / Organic.</p>
        <table class="data-table" id="sessions-table">
            <thead>
                <tr>
                    <th>Channel</th>
                    <th class="right">Sessions</th>
                    <th class="right">Purchases</th>
                    <th class="right">CVR</th>
                </tr>
            </thead>
            <tbody id="sessions-rows"></tbody>
        </table>
    </div>
    <div class="section">
        <h2>New Customers &amp; CAC</h2>
        <p class="note">First-time Shopify buyers attributed via GA4. Approximate.</p>
        <table class="data-table" id="new-cust-table">
            <thead>
                <tr>
                    <th>Channel</th>
                    <th class="right">New Cust.</th>
                    <th class="right">MoM</th>
                </tr>
            </thead>
            <tbody id="new-cust-rows"></tbody>
        </table>
    </div>
</div>

<!-- -- Trailing Revenue + AOV -- -->
<div class="section">
    <h2>Revenue &amp; AOV (Trailing 13 Months)</h2>
    <div id="chart-revenue-aov" class="chart-container"></div>
</div>

<!-- -- Revenue by Source (Paid vs Organic) -- -->
<div class="section">
    <h2>Revenue by Source — Paid vs Organic</h2>
    <p class="note">Shopify revenue attributed via GA4 sessions. Review Site = referral traffic from pickleballeffect.com.</p>
    <div id="chart-revenue-source" class="chart-container"></div>
</div>

<!-- -- Weekly Trends + Monthly ROAS Trends -- -->
<div class="two-col">
    <div class="section">
        <h2>Weekly Spend &amp; Revenue (13 wk)</h2>
        <div id="chart-weekly" class="chart-container"></div>
    </div>
    <div class="section">
        <h2>Monthly ROAS by Channel (13 mo)</h2>
        <div id="chart-monthly-roas" class="chart-container"></div>
    </div>
</div>

<!-- -- Conversion Funnel -- -->
<div class="section">
    <h2>Conversion Funnel</h2>
    <div id="chart-funnel" class="chart-container" style="max-width:600px; margin:0 auto;"></div>
</div>

<!-- -- Google Ads Health -- -->
<div class="section">
    <h2>Google Ads Keyword Health</h2>
    <table class="health-table">
        <thead>
            <tr>
                <th>Keyword</th>
                <th>Campaign</th>
                <th style="text-align:right">Spend</th>
                <th style="text-align:right">ROAS</th>
                <th style="text-align:center">QS</th>
                <th>Tier</th>
            </tr>
        </thead>
        <tbody id="keyword-rows"></tbody>
    </table>
</div>

</div><!-- .container -->

<!-- -- Footer -- -->
<div class="footer">
    Generated $generated_at &nbsp;|&nbsp; Pickleball Effect — Monthly Performance Dashboard
</div>

<!-- -- Data & Charts -- -->
<script>
// Embedded data
var DATA = {
    channelKpis: $channel_kpis_json,
    newCustomers: $new_customers_json,
    sessionsCvr: $sessions_cvr_json,
    funnel: $funnel_json,
    keywords: $keywords_json,
    pnl: $pnl_json,
    revenueAov: $revenue_aov_json,
    weekly: $weekly_json,
    monthlyRoas: $monthly_roas_json,
    revenueBySource: $revenue_by_source_json
};

// Helpers
function fmt$$$(v) { return '$$' + v.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}); }
function fmtPct(v) { return v.toFixed(2) + '%'; }
function fmtNum(v) { return v.toLocaleString(undefined, {maximumFractionDigits: 1}); }
function fmtInt(v) { return v.toLocaleString(undefined, {maximumFractionDigits: 0}); }
function momArrow(cur, prev, invert) {
    if (!prev) return '<span class="mom-flat">--</span>';
    var pct = ((cur - prev) / Math.abs(prev) * 100).toFixed(1);
    var sign = pct > 0 ? '+' : '';
    var cls = Math.abs(pct) < 1 ? 'mom-flat' : ((pct > 0) !== (invert || false) ? 'mom-up' : 'mom-down');
    return '<span class="' + cls + '">' + sign + pct + '%</span>';
}

// Highcharts global defaults
Highcharts.setOptions({
    chart: {
        style: { fontFamily: 'Inter, -apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif' }
    },
    credits: { enabled: false },
    colors: ['#667eea', '#764ba2', '#f093fb', '#f5576c', '#4facfe', '#00f2fe', '#43e97b', '#fa709a']
});

// -- P&L Table --
(function() {
    var d = DATA.pnl;
    // Header row: metric labels as rows, months as columns
    var thead = document.getElementById('pnl-thead');
    var htr = document.createElement('tr');
    htr.innerHTML = '<th></th>';
    d.months.forEach(function(m) {
        // Format YYYY-MM to short label
        var parts = m.split('-');
        var dt = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1);
        var label = dt.toLocaleDateString('en-US', {month: 'short', year: '2-digit'});
        htr.innerHTML += '<th style="text-align:right">' + label + '</th>';
    });
    htr.innerHTML += '<th style="text-align:right">Total</th>';
    thead.appendChild(htr);

    // Row labels and keys
    var metrics = [
        {label: 'Gross Sales', key: 'gross_revenue'},
        {label: 'Discounts', key: 'discounts', cost: true},
        {label: 'Shipping Collected', key: 'shipping_collected'},
        {label: 'Total Revenue', key: 'total_revenue', bold: true},
        {label: 'Partnership Revenue (60%)', key: 'partnership_slice', cost: true},
        {label: 'Net Revenue', key: 'net_revenue'},
        {label: 'COGS', key: 'cogs', cost: true},
        {label: 'Shipping Cost', key: 'shipping_cost', cost: true},
        {label: 'Gross Profit', key: 'gross_profit', bold: true},
        {label: 'Ad Spend', key: 'ad_spend', cost: true},
        {label: 'Agency Fee', key: 'agency_fee', cost: true},
        {label: 'Bottom Line', key: 'bottom_line'}
    ];

    var tbody = document.getElementById('pnl-tbody');
    metrics.forEach(function(m) {
        var tr = document.createElement('tr');
        var isBottomLine = m.key === 'bottom_line';
        var isCost = m.cost || false;
        var isBold = m.bold || isBottomLine;
        if (isBold) {
            tr.style.fontWeight = '700';
        }
        if (isBottomLine || m.key === 'total_revenue' || m.key === 'gross_profit') {
            tr.style.borderTop = '2px solid #e5e7eb';
        }
        var html = '<td>' + m.label + '</td>';
        d.rows.forEach(function(row) {
            var v = row[m.key];
            var cls = '';
            if (isBottomLine) cls = v >= 0 ? 'pnl-positive' : 'pnl-negative';
            html += '<td style="text-align:right" class="' + cls + '">' +
                    (isCost ? '(' + fmt$$$(Math.abs(v)) + ')' : fmt$$$(v)) + '</td>';
        });
        // Totals column
        var tv = d.totals[m.key];
        var tcls = '';
        if (isBottomLine) tcls = tv >= 0 ? 'pnl-positive' : 'pnl-negative';
        html += '<td style="text-align:right" class="' + tcls + '"><strong>' +
                (isCost ? '(' + fmt$$$(Math.abs(tv)) + ')' : fmt$$$(tv)) + '</strong></td>';
        tr.innerHTML = html;
        tbody.appendChild(tr);
    });
})();

// -- Channel KPIs Table --
(function() {
    var d = DATA.channelKpis;
    var tbody = document.getElementById('channel-kpis-rows');
    d.channels.forEach(function(ch) {
        var t = d.data[ch] && d.data[ch].target || {};
        var p = d.data[ch] && d.data[ch].prior || {};
        if (!t.spend && !t.revenue && ch !== 'Email') return; // skip empty channels
        var tr = document.createElement('tr');
        tr.innerHTML =
            '<td><strong>' + ch + '</strong></td>' +
            '<td class="right">' + fmt$$$(t.spend || 0) + ' ' + momArrow(t.spend||0, p.spend||0) + '</td>' +
            '<td class="right">' + fmtNum(t.clicks || 0) + '</td>' +
            '<td class="right">' + fmtPct(t.ctr || 0) + '</td>' +
            '<td class="right">' + fmtNum(t.conversions || 0) + '</td>' +
            '<td class="right">' + fmt$$$(t.revenue || 0) + ' ' + momArrow(t.revenue||0, p.revenue||0) + '</td>' +
            '<td class="right">' + (t.roas || 0).toFixed(2) + 'x ' + momArrow(t.roas||0, p.roas||0) + '</td>' +
            '<td class="right">' + fmt$$$(t.cpa || 0) + ' ' + momArrow(t.cpa||0, p.cpa||0, true) + '</td>' +
            '<td class="right">' + fmt$$$(t.aov || 0) + '</td>';
        tbody.appendChild(tr);
    });
})();

// -- Sessions & CVR Table --
(function() {
    var d = DATA.sessionsCvr;
    var tbody = document.getElementById('sessions-rows');
    var channels = ['Meta', 'Google CPC (Paid)', 'Google Product Sync', 'Google Organic', 'Email', 'Other'];
    channels.forEach(function(ch) {
        var t = d[ch] && d[ch].target || {};
        var p = d[ch] && d[ch].prior || {};
        if (!t.sessions) return;
        var tr = document.createElement('tr');
        tr.innerHTML =
            '<td><strong>' + ch + '</strong></td>' +
            '<td class="right">' + fmtNum(t.sessions || 0) + ' ' + momArrow(t.sessions||0, p.sessions||0) + '</td>' +
            '<td class="right">' + fmtNum(t.purchases || 0) + '</td>' +
            '<td class="right">' + fmtPct(t.cvr || 0) + ' ' + momArrow(t.cvr||0, p.cvr||0) + '</td>';
        tbody.appendChild(tr);
    });
})();

// -- New Customers Table --
(function() {
    var d = DATA.newCustomers;
    var tbody = document.getElementById('new-cust-rows');
    var channels = ['Meta', 'Search Brand', 'Search Non-Brand', 'Shopping', 'Email', 'Other'];
    var totalT = 0, totalP = 0;
    channels.forEach(function(ch) {
        var t = d[ch] ? d[ch].target || 0 : 0;
        var p = d[ch] ? d[ch].prior || 0 : 0;
        totalT += t; totalP += p;
        if (!t && !p) return;
        var tr = document.createElement('tr');
        tr.innerHTML =
            '<td><strong>' + ch + '</strong></td>' +
            '<td class="right">' + t + '</td>' +
            '<td class="right">' + momArrow(t, p) + '</td>';
        tbody.appendChild(tr);
    });
    // Total row
    var tfr = document.createElement('tr');
    tfr.style.fontWeight = '700';
    tfr.style.borderTop = '2px solid #e5e7eb';
    tfr.innerHTML =
        '<td>Total</td>' +
        '<td class="right">' + totalT + '</td>' +
        '<td class="right">' + momArrow(totalT, totalP) + '</td>';
    tbody.appendChild(tfr);
})();

// -- Revenue + AOV (Trailing 13 Months) --
(function() {
    var d = DATA.revenueAov;
    Highcharts.chart('chart-revenue-aov', {
        chart: { type: 'column' },
        title: { text: null },
        xAxis: {
            categories: d.months,
            crosshair: true
        },
        yAxis: [
            { title: { text: 'Revenue ($$)' }, labels: { format: '$${value:,.0f}' } },
            { title: { text: 'AOV ($$)' }, opposite: true, min: 0, labels: { format: '$${value:,.0f}' } }
        ],
        tooltip: { shared: true },
        plotOptions: { column: { borderRadius: 4 } },
        series: [
            { name: 'Revenue', data: d.revenue, yAxis: 0, color: '#22c55e' },
            { name: 'AOV', data: d.aov, type: 'line', yAxis: 1, color: '#667eea', marker: { radius: 4 }, lineWidth: 2 }
        ]
    });
})();

// -- Revenue by Source (Paid vs Organic) --
(function() {
    var d = DATA.revenueBySource;
    Highcharts.chart('chart-revenue-source', {
        chart: { type: 'column' },
        title: { text: null },
        xAxis: {
            categories: d.months,
            crosshair: true
        },
        yAxis: {
            title: { text: 'Revenue ($$)' },
            labels: { format: '$${value:,.0f}' },
            stackLabels: {
                enabled: true,
                style: { fontWeight: '600', color: '#374151', fontSize: '11px' },
                format: '$${total:,.0f}'
            }
        },
        tooltip: {
            shared: true,
            valuePrefix: '$$',
            pointFormat: '<span style="color:{series.color}">\u25CF</span> {series.name}: <b>$${point.y:,.0f}</b> ({point.percentage:.0f}%)<br/>'
        },
        plotOptions: {
            column: {
                stacking: 'normal',
                borderRadius: 2
            }
        },
        series: d.series
    });
})();

// -- Weekly Spend & Revenue (13 wk) --
(function() {
    var d = DATA.weekly;
    Highcharts.chart('chart-weekly', {
        chart: { type: 'line' },
        title: { text: null },
        xAxis: { categories: d.weeks, labels: { step: Math.ceil(d.weeks.length / 8) } },
        yAxis: { title: { text: 'Dollars ($$)' }, labels: { format: '$${value:,.0f}' } },
        tooltip: { shared: true, valuePrefix: '$$' },
        series: [
            { name: 'Spend', data: d.spend, color: '#667eea', lineWidth: 2 },
            { name: 'Revenue', data: d.revenue, color: '#22c55e', lineWidth: 2 }
        ]
    });
})();

// -- Monthly ROAS by Channel (13 mo) --
(function() {
    var d = DATA.monthlyRoas;
    Highcharts.chart('chart-monthly-roas', {
        chart: { type: 'line' },
        title: { text: null },
        xAxis: { categories: d.months },
        yAxis: {
            title: { text: 'ROAS' },
            plotLines: [{
                value: 1.0, color: '#ef4444', width: 2, dashStyle: 'Dash',
                label: { text: 'Breakeven', style: { color: '#ef4444', fontSize: '11px' } }
            }]
        },
        tooltip: { shared: true, valueSuffix: 'x' },
        series: d.series
    });
})();

// -- Conversion Funnel --
(function() {
    var d = DATA.funnel;
    Highcharts.chart('chart-funnel', {
        chart: { type: 'funnel' },
        title: { text: null },
        plotOptions: {
            series: {
                neckWidth: '30%',
                neckHeight: '25%',
                dataLabels: {
                    enabled: true,
                    format: '<b>{point.name}</b>: {point.y:,.0f}',
                    softConnector: true,
                    style: { fontSize: '13px' }
                }
            }
        },
        series: [{ name: 'Funnel', data: d }]
    });
})();

// -- Google Ads Health Table --
(function() {
    var rows = DATA.keywords;
    var tbody = document.getElementById('keyword-rows');
    rows.forEach(function(r) {
        var tierClass = 'tier-' + (r.tier || '').replace(/\\s+/g, '_').toLowerCase();
        var tr = document.createElement('tr');
        tr.innerHTML =
            '<td>' + r.keyword + '</td>' +
            '<td>' + r.campaign + '</td>' +
            '<td style="text-align:right">$$' + r.spend.toFixed(2) + '</td>' +
            '<td style="text-align:right">' + r.roas.toFixed(2) + 'x</td>' +
            '<td style="text-align:center">' + (r.qs || '--') + '</td>' +
            '<td><span class="tier-badge ' + tierClass + '">' + (r.tier || '--') + '</span></td>';
        tbody.appendChild(tr);
    });
})();
</script>
</body>
</html>
""")
