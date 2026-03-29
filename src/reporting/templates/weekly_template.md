# WEEKLY MARKETING PERFORMANCE SUMMARY
**Period:** {{ week_start }} to {{ week_end }} ({{ week_label }})
**Generated:** {{ generated_at }}

---

## EXECUTIVE SUMMARY

| Metric | This Week | Last Week | WoW Change |
|--------|-----------|-----------|------------|
| Total Ad Spend | ${{ "%.0f"|format(total_spend) }} | ${{ "%.0f"|format(prev_week_spend|default(0)) }} | {% if spend_wow is defined and spend_wow is not none %}{{ "▲" if spend_wow > 0 else "▼" }} {{ "%.1f"|format(spend_wow|abs) }}%{% else %}—{% endif %} |
| Total Conversions | {{ total_conversions|default(0) }} | {{ prev_week_conversions|default("—") }} | {% if conversions_wow is defined and conversions_wow is not none %}{{ "▲" if conversions_wow > 0 else "▼" }} {{ "%.1f"|format(conversions_wow|abs) }}%{% else %}—{% endif %} |
| Blended CPA | ${{ "%.2f"|format(blended_cpa|default(0)) }} | — | — |
| Total Sessions | {{ "%.0f"|format(total_sessions|default(0)) }} | — | — |
| MQLs Generated | {{ mqls_generated|default(0) }} | {{ prev_week_mqls|default("—") }} | — |

---

## CHANNEL PERFORMANCE BREAKDOWN

| Channel | Spend | Conversions | CPA | ROAS | Sessions |
|---------|-------|-------------|-----|------|---------|
{% for ch in channel_breakdown %}
| {{ ch.channel }} | ${{ "%.0f"|format(ch.spend) }} | {{ ch.conversions }} | ${{ "%.2f"|format(ch.cpa) }} | {{ "%.2f"|format(ch.roas) }}x | {{ ch.sessions }} |
{% else %}
| No channel data available | — | — | — | — | — |
{% endfor %}

---

## TOP 5 CAMPAIGNS BY CONVERSIONS

| # | Campaign | Platform | Spend | Conversions | CPA | ROAS |
|---|----------|----------|-------|-------------|-----|------|
{% for c in top_campaigns[:5] %}
| {{ loop.index }} | {{ c.campaign_name }} | {{ c.platform }} | ${{ "%.0f"|format(c.total_spend) }} | {{ c.total_conversions }} | ${{ "%.2f"|format(c.cpa) }} | {{ "%.2f"|format(c.roas) }}x |
{% else %}
| — | No data | — | — | — | — | — |
{% endfor %}

---

## PAUSE CANDIDATES (Bottom 5 by Efficiency)

| Campaign | Platform | Spend | Conversions | CPA | Recommendation |
|----------|----------|-------|-------------|-----|----------------|
{% for c in bottom_campaigns[:5] %}
| {{ c.campaign_name }} | {{ c.platform }} | ${{ "%.0f"|format(c.total_spend) }} | {{ c.total_conversions }} | ${{ "%.2f"|format(c.cpa) }} | {{ c.recommendation }} |
{% else %}
| — | No inefficient campaigns identified | — | — | — | — |
{% endfor %}

---

## FUNNEL SNAPSHOT

```
MQLs Generated    →    SQLs    →    Opportunities    →    Closed Won
{{ mqls_generated }}                  {{ sqls_generated if sqls_generated is defined else "—" }}                {{ opportunities_created if opportunities_created is defined else "—" }}                  {{ closed_won if closed_won is defined else "—" }}
```

**Lead Velocity Rate (LVR):** {{ lvr if lvr is defined else "N/A" }}

---

## EMAIL PERFORMANCE

| Campaign Type | Sends | Delivery | Open Rate | CTR | Conversions |
|---------------|-------|----------|-----------|-----|-------------|
{% for e in email_summary %}
| {{ e.segment_name }} | {{ e.total_sends }} | {{ "%.1f"|format(e.delivery_rate) }}% | {{ "%.1f"|format(e.open_rate) }}% | {{ "%.1f"|format(e.click_rate) }}% | {{ e.conversions }} |
{% else %}
| All Campaigns | {{ "%.0f"|format(total_email_sends|default(0)) }} | — | — | — | — |
{% endfor %}

---

## ANOMALIES DETECTED THIS WEEK

{% if anomalies %}
{% for a in anomalies %}
- **{{ a.severity }}** — {{ a.metric }}: Observed {{ a.observed_value }}, Expected ~{{ a.expected_value }}. *{{ a.recommendation }}*
{% endfor %}
{% else %}
No anomalies detected this week. ✅
{% endif %}

---

## BUDGET SHIFT RECOMMENDATIONS

{% if recommendations %}
{% for r in recommendations %}
- **{{ r.channel }}** (${{ "%.0f"|format(r.total_spend) }} spend, {{ "%.2f"|format(r.roas) }}x ROAS): {{ r.recommendation }}
{% endfor %}
{% else %}
No budget shifts recommended at this time.
{% endif %}

---

**DATA QUALITY SCORE: {{ dq_score }}/100** {% if dq_score >= 90 %}✅{% elif dq_score >= 70 %}⚠️{% else %}❌{% endif %}

*Generated: {{ generated_at }}*
