━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DAILY MARKETING PULSE — {{ report_date }}
Data as of: {{ generated_at }} UTC | Pipeline Status: {% if pipeline_status == 'HEALTHY' %}✅ HEALTHY{% elif pipeline_status == 'WARNING' %}⚠️ WARNING{% else %}❌ {{ pipeline_status }}{% endif %}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## SPEND & EFFICIENCY

| Metric | Value | vs 7-day Avg |
|--------|-------|-------------|
| Total Spend Today | ${{ "%.0f"|format(total_spend) }} | {% if spend_vs_avg is defined %}{{ "▲" if spend_vs_avg > 0 else "▼" }} {{ "%.0f"|format(spend_vs_avg|abs) }}%{% else %}—{% endif %} |
| Meta Ads | ${{ "%.0f"|format(meta_spend) }} | — |
| Google Ads | ${{ "%.0f"|format(google_spend) }} | — |
| Blended CPC | ${{ "%.2f"|format(blended_cpc) }} | — |
| Blended CPA | ${{ "%.2f"|format(blended_cpa) }} | — |

## TRAFFIC & ENGAGEMENT

| Metric | Value | vs 7-day Avg |
|--------|-------|-------------|
| Total Sessions | {{ "%.0f"|format(total_sessions|int) }} | — |
| Paid Sessions | {{ "%.0f"|format(paid_sessions|int) }} | — |
| Organic Sessions | {{ "%.0f"|format(organic_sessions|int) }} | — |
| Bounce Rate | {{ "%.1f"|format(bounce_rate) }}% | — |
| Avg Session Duration | {{ avg_session_duration }} | — |

## CONVERSIONS & PIPELINE

| Metric | Value |
|--------|-------|
| Demo Requests | {{ demo_requests }} |
| Trial Signups | {{ trial_signups }} |
| MQLs Generated | {{ mqls_generated }} |
| Opportunities Created | {{ opportunities_created }} |
| Pipeline Value Added | ${{ "%.0f"|format(pipeline_value_added|int) }} |

## EMAIL PERFORMANCE

| Metric | Value |
|--------|-------|
| Sends Today | {{ "%.0f"|format(email_sends|int) }} |
| Delivery Rate | {{ "%.1f"|format(delivery_rate) }}% {% if delivery_rate >= 95 %}✅ healthy{% endif %} |
| Open Rate | {{ "%.1f"|format(open_rate) }}% |
| Click Rate | {{ "%.1f"|format(click_rate) }}% |

{% if alerts %}
## ⚠️ ALERTS

{% for alert in alerts %}
  • {{ alert }}
{% endfor %}
{% endif %}

---
**DATA QUALITY: {{ dq_score }}/100** {% if dq_score >= 90 %}✅{% elif dq_score >= 70 %}⚠️{% else %}❌{% endif %}

*Generated: {{ generated_at }} | Pipeline Run: Automated Daily*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
