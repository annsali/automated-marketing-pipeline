# MONTHLY MARKETING EXECUTIVE REPORT
**Period:** {{ month_label }} ({{ month_start }} – {{ month_end }})
**Generated:** {{ generated_at }}
**Data Quality:** {{ dq_score }}/100 {% if dq_score >= 90 %}✅{% elif dq_score >= 70 %}⚠️{% else %}❌{% endif %}

---

## EXECUTIVE SUMMARY

{% if pipeline_status == 'HEALTHY' %}✅{% else %}⚠️{% endif %} Pipeline Status: **{{ pipeline_status }}**

This month, total marketing investment was **${{ "%.0f"|format(total_spend) }}**, generating **{{ total_conversions }} conversions** at a blended CPA of **${{ "%.2f"|format(blended_cpa) }}** and ROAS of **{{ "%.2f"|format(blended_roas) }}x**.

### Key Metrics vs Prior Month

| Metric | This Month | Prior Month | MoM Change | YoY Change |
|--------|-----------|-------------|-----------|-----------|
| Total Ad Spend | ${{ "%.0f"|format(total_spend) }} | ${{ "%.0f"|format(prev_month_spend|default(0)) }} | {{ "▲" if spend_mom > 0 else "▼" }}{{ "%.1f"|format(spend_mom|abs) }}% | {{ "▲" if spend_yoy > 0 else "▼" }}{{ "%.1f"|format(spend_yoy|abs) }}% |
| Total Conversions | {{ total_conversions }} | {{ prev_month_conversions|default("—") }} | {{ "▲" if conversions_mom > 0 else "▼" }}{{ "%.1f"|format(conversions_mom|abs) }}% | — |
| Blended CPA | ${{ "%.2f"|format(blended_cpa) }} | — | — | — |
| Blended ROAS | {{ "%.2f"|format(blended_roas) }}x | — | — | — |
| Total Sessions | {{ "%.0f"|format(total_sessions|default(0)) }} | — | — | — |
| MQLs Generated | {{ mqls_generated }} | — | — | — |

---

## CHANNEL ATTRIBUTION (Time-Decay Model)

| Channel | Attributed Conversions | Attribution Share | Sessions | Conv. Rate |
|---------|----------------------|------------------|---------|-----------|
{% for ch in channel_attribution %}
| {{ ch.channel }} | {{ "%.1f"|format(ch.attributed_conversions) }} | {{ "%.1f"|format(ch.attribution_share_pct) }}% | {{ ch.total_sessions }} | {{ "%.2f"|format(ch.conversion_rate) }}% |
{% else %}
| No attribution data available | — | — | — | — |
{% endfor %}

---

## PIPELINE FORECAST

**Total Open Pipeline:** ${{ "%.0f"|format(total_pipeline_value|default(0)) }}
**Weighted Forecast (Expected):** ${{ "%.0f"|format(total_expected_value|default(0)) }}

| Stage | Opportunities | Pipeline Value | Win Probability | Expected Value |
|-------|--------------|----------------|----------------|---------------|
{% for stage in pipeline_forecast %}
| {{ stage.stage }} | {{ stage.opportunity_count }} | ${{ "%.0f"|format(stage.total_pipeline_value|int) }} | {{ stage.win_probability_pct }}% | ${{ "%.0f"|format(stage.expected_value|int) }} |
{% else %}
| No pipeline data | — | — | — | — |
{% endfor %}

---

## COHORT PERFORMANCE UPDATE

| Cohort Month | Cohort Size | MQLs | SQLs | Customers | MQL Rate | Cohort Conv. Rate |
|-------------|------------|------|------|-----------|----------|-----------------|
{% for cohort in cohort_data[:6] %}
| {{ cohort.cohort_month }} | {{ cohort.cohort_size }} | {{ cohort.mqls }} | {{ cohort.sqls }} | {{ cohort.customers }} | {{ cohort.mql_rate_pct }}% | {{ cohort.cohort_conversion_rate_pct }}% |
{% else %}
| No cohort data | — | — | — | — | — | — |
{% endfor %}

---

## BUDGET UTILIZATION

| Platform | Channel | Actual Spend | Benchmark | Utilization | Status |
|----------|---------|-------------|-----------|------------|--------|
{% for b in budget_utilization %}
| {{ b.platform }} | {{ b.channel }} | ${{ "%.0f"|format(b.actual_spend) }} | ${{ "%.0f"|format(b.benchmark_spend|default(0)) }} | {{ b.utilization_pct|default(0) }}% | {{ b.budget_status }} |
{% else %}
| No budget data | — | — | — | — | — |
{% endfor %}

---

## KEY WINS & LOSSES THIS MONTH

### Top Deals

| Account | Stage | Amount | Product | Days to Close |
|---------|-------|--------|---------|--------------|
{% for deal in top_deals[:5] %}
| {{ deal.account_name|default("—") }} | {{ deal.stage }} | ${{ "%.0f"|format(deal.amount|int) }} | {{ deal.product_line }} | {{ deal.days_to_close|default("—") }} |
{% else %}
| No deal data available | — | — | — | — |
{% endfor %}

---

## LEAD GENERATION BY SOURCE

| Source | New Leads | MQLs | MQL Rate | MoM Change |
|--------|-----------|------|----------|-----------|
{% for src in lead_sources[:8] %}
| {{ src.lead_source }} | {{ src.new_leads }} | {{ src.mqls }} | {{ src.mql_rate }}% | {% if src.leads_mom_pct is not none %}{{ "▲" if src.leads_mom_pct > 0 else "▼" }}{{ "%.1f"|format(src.leads_mom_pct|abs) }}%{% else %}—{% endif %} |
{% else %}
| No lead source data | — | — | — | — |
{% endfor %}

---

## STRATEGIC RECOMMENDATIONS FOR NEXT MONTH

{% if recommendations %}
{% for rec in recommendations %}
{{ loop.index }}. **{{ rec.priority }}** — {{ rec.recommendation }}
{% endfor %}
{% else %}
1. **Review channel mix** — Evaluate top-performing channels for budget increase
2. **Creative refresh** — Monitor ad fatigue indicators across Meta and Google campaigns
3. **Pipeline acceleration** — Focus on opportunities in Proposal/Negotiation stage
4. **Email list hygiene** — Review and clean segments with high bounce rates
{% endif %}

---

## DATA QUALITY SUMMARY

| Table | Total Rows | Latest Date | Data Age | Status |
|-------|-----------|------------|---------|--------|
{% for dq in dq_table_summary %}
| {{ dq.table_name }} | {{ "%.0f"|format(dq.total_rows|int) }} | {{ dq.latest_date|default("—") }} | {{ dq.data_age_days|default("—") }} days | {% if dq.data_age_days is not none and dq.data_age_days <= 1 %}✅{% else %}⚠️{% endif %} |
{% else %}
| No DQ data available | — | — | — | — |
{% endfor %}

**Overall DQ Score:** {{ dq_score }}/100

---

*This report was auto-generated by the Marketing Data Pipeline*
*Report Period: {{ month_label }} | Generated: {{ generated_at }}*
