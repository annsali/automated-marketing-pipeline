-- =============================================================================
-- Monthly Report SQL Queries
-- Marketing Data Pipeline - Monthly Executive Report
-- =============================================================================

-- Query 1: Monthly Spend & Performance - MoM and YoY Comparison
WITH monthly_perf AS (
    SELECT
        strftime('%Y-%m', date) AS month_label,
        strftime('%Y', date) AS year,
        strftime('%m', date) AS month_num,
        SUM(spend) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(conversions) AS total_conversions,
        SUM(conversion_value) AS total_conversion_value,
        CASE WHEN SUM(clicks) > 0 THEN ROUND(SUM(spend) / SUM(clicks), 2) ELSE 0 END AS blended_cpc,
        CASE WHEN SUM(conversions) > 0 THEN ROUND(SUM(spend) / SUM(conversions), 2) ELSE 0 END AS blended_cpa,
        CASE WHEN SUM(spend) > 0 THEN ROUND(SUM(conversion_value) / SUM(spend), 2) ELSE 0 END AS blended_roas,
        CASE WHEN SUM(impressions) > 0 THEN ROUND(CAST(SUM(clicks) AS REAL) / SUM(impressions) * 100, 4) ELSE 0 END AS blended_ctr
    FROM fact_ad_performance
    GROUP BY strftime('%Y-%m', date)
),
mom_yoy AS (
    SELECT
        m.*,
        LAG(m.total_spend) OVER (ORDER BY m.month_label) AS prev_month_spend,
        LAG(m.total_conversions) OVER (ORDER BY m.month_label) AS prev_month_conversions,
        -- Year-over-year (12 months ago)
        LAG(m.total_spend, 12) OVER (ORDER BY m.month_label) AS prev_year_spend,
        LAG(m.total_conversions, 12) OVER (ORDER BY m.month_label) AS prev_year_conversions,
        ROUND(
            (m.total_spend - LAG(m.total_spend) OVER (ORDER BY m.month_label)) /
            NULLIF(LAG(m.total_spend) OVER (ORDER BY m.month_label), 0) * 100, 2
        ) AS spend_mom_pct,
        ROUND(
            (m.total_spend - LAG(m.total_spend, 12) OVER (ORDER BY m.month_label)) /
            NULLIF(LAG(m.total_spend, 12) OVER (ORDER BY m.month_label), 0) * 100, 2
        ) AS spend_yoy_pct,
        ROUND(
            (m.total_conversions - LAG(m.total_conversions) OVER (ORDER BY m.month_label)) /
            NULLIF(LAG(m.total_conversions) OVER (ORDER BY m.month_label), 0) * 100, 2
        ) AS conversions_mom_pct
    FROM monthly_perf m
)
SELECT
    month_label,
    ROUND(total_spend, 2) AS total_spend,
    total_impressions,
    total_clicks,
    total_conversions,
    ROUND(total_conversion_value, 2) AS total_conversion_value,
    blended_cpc,
    blended_cpa,
    blended_roas,
    blended_ctr,
    spend_mom_pct,
    spend_yoy_pct,
    conversions_mom_pct
FROM mom_yoy
ORDER BY month_label DESC
LIMIT 13;


-- Query 2: Monthly Channel Attribution Summary
-- Time-decay attribution model - later touches get more credit
WITH session_touchpoints AS (
    SELECT
        s.session_id,
        s.master_id,
        s.date,
        s.channel_group AS channel,
        s.converted,
        s.conversion_type,
        -- Assign decay weight (more recent = higher weight)
        ROW_NUMBER() OVER (PARTITION BY s.master_id ORDER BY s.date DESC) AS touch_recency_rank,
        COUNT(*) OVER (PARTITION BY s.master_id) AS total_touches
    FROM fact_web_sessions s
    WHERE s.date >= date('now', '-30 days')
),
time_decay_attribution AS (
    SELECT
        channel,
        SUM(
            CASE WHEN converted = 1
                THEN POWER(0.7, touch_recency_rank - 1) / (1 - POWER(0.7, total_touches)) * 1.0
                ELSE 0
            END
        ) AS attributed_conversions,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNT(DISTINCT master_id) AS unique_users
    FROM session_touchpoints
    GROUP BY channel
)
SELECT
    channel,
    ROUND(attributed_conversions, 2) AS attributed_conversions,
    total_sessions,
    unique_users,
    ROUND(CAST(attributed_conversions AS REAL) / NULLIF(total_sessions, 0) * 100, 4) AS conversion_rate,
    ROUND(attributed_conversions / NULLIF(SUM(attributed_conversions) OVER (), 0) * 100, 2) AS attribution_share_pct
FROM time_decay_attribution
ORDER BY attributed_conversions DESC;


-- Query 3: Pipeline Forecast
-- Projects close amounts based on stage + historical win rates
WITH pipeline_by_stage AS (
    SELECT
        stage,
        COUNT(*) AS opportunity_count,
        SUM(amount) AS total_pipeline_value,
        AVG(win_probability) AS avg_win_probability,
        ROUND(SUM(amount) * AVG(win_probability), 2) AS expected_value,
        -- Days to close based on average sales cycle
        AVG(sales_cycle_days) AS avg_days_to_close
    FROM fact_pipeline
    WHERE is_won = 0
        AND stage NOT LIKE '%Closed%'
    GROUP BY stage
),
forecast_summary AS (
    SELECT
        stage,
        opportunity_count,
        ROUND(total_pipeline_value, 2) AS total_pipeline_value,
        ROUND(avg_win_probability * 100, 1) AS win_probability_pct,
        expected_value,
        ROUND(avg_days_to_close, 0) AS avg_days_to_close,
        SUM(expected_value) OVER () AS total_expected_value
    FROM pipeline_by_stage
)
SELECT
    stage,
    opportunity_count,
    total_pipeline_value,
    win_probability_pct,
    expected_value,
    avg_days_to_close,
    ROUND(expected_value / NULLIF(total_expected_value, 0) * 100, 2) AS share_of_forecast_pct
FROM forecast_summary
ORDER BY win_probability_pct DESC;


-- Query 4: Monthly Cohort Performance Update
-- How are recent acquisition cohorts converting over time?
WITH contact_cohorts AS (
    SELECT
        c.contact_id,
        c.master_id,
        strftime('%Y-%m', c.created_date) AS cohort_month,
        c.lead_status,
        c.mql_date,
        c.sql_date,
        c.lead_source
    FROM dim_contacts c
    WHERE c.created_date IS NOT NULL
),
cohort_conversions AS (
    SELECT
        cc.cohort_month,
        COUNT(DISTINCT cc.contact_id) AS cohort_size,
        SUM(CASE WHEN cc.lead_status IN ('MQL', 'SQL', 'Opportunity', 'Customer') THEN 1 ELSE 0 END) AS mqls,
        SUM(CASE WHEN cc.lead_status IN ('SQL', 'Opportunity', 'Customer') THEN 1 ELSE 0 END) AS sqls,
        SUM(CASE WHEN cc.lead_status = 'Customer' THEN 1 ELSE 0 END) AS customers,
        COUNT(DISTINCT fp.opp_id) AS opportunities,
        COALESCE(SUM(fp.amount), 0) AS total_opportunity_value
    FROM contact_cohorts cc
    LEFT JOIN fact_pipeline fp ON cc.master_id = fp.master_id
    GROUP BY cc.cohort_month
)
SELECT
    cohort_month,
    cohort_size,
    mqls,
    sqls,
    customers,
    opportunities,
    ROUND(total_opportunity_value, 2) AS total_opportunity_value,
    ROUND(CAST(mqls AS REAL) / NULLIF(cohort_size, 0) * 100, 2) AS mql_rate_pct,
    ROUND(CAST(sqls AS REAL) / NULLIF(mqls, 0) * 100, 2) AS mql_to_sql_rate_pct,
    ROUND(CAST(customers AS REAL) / NULLIF(cohort_size, 0) * 100, 2) AS cohort_conversion_rate_pct
FROM cohort_conversions
ORDER BY cohort_month DESC
LIMIT 12;


-- Query 5: Monthly Budget Utilization
-- Actual vs planned spend by platform/channel
WITH monthly_actual AS (
    SELECT
        strftime('%Y-%m', date) AS month_label,
        platform,
        channel,
        SUM(spend) AS actual_spend,
        SUM(impressions) AS actual_impressions,
        SUM(conversions) AS actual_conversions
    FROM fact_ad_performance
    WHERE strftime('%Y-%m', date) = strftime('%Y-%m', 'now')
    GROUP BY strftime('%Y-%m', date), platform, channel
),
-- Simulate planned budget (based on 30-day rolling average * days in month)
monthly_benchmark AS (
    SELECT
        platform,
        channel,
        ROUND(AVG(daily_spend) * 30, 2) AS benchmark_monthly_spend
    FROM (
        SELECT
            platform,
            channel,
            date,
            SUM(spend) AS daily_spend
        FROM fact_ad_performance
        WHERE date >= date('now', '-90 days')
        GROUP BY platform, channel, date
    ) daily_data
    GROUP BY platform, channel
)
SELECT
    ma.month_label,
    ma.platform,
    ma.channel,
    ROUND(ma.actual_spend, 2) AS actual_spend,
    ROUND(mb.benchmark_monthly_spend, 2) AS benchmark_spend,
    ROUND(ma.actual_spend / NULLIF(mb.benchmark_monthly_spend, 0) * 100, 2) AS utilization_pct,
    ma.actual_conversions,
    CASE
        WHEN ma.actual_spend / NULLIF(mb.benchmark_monthly_spend, 0) > 1.1 THEN 'OVER BUDGET'
        WHEN ma.actual_spend / NULLIF(mb.benchmark_monthly_spend, 0) < 0.9 THEN 'UNDER BUDGET'
        ELSE 'ON TRACK'
    END AS budget_status
FROM monthly_actual ma
LEFT JOIN monthly_benchmark mb ON ma.platform = mb.platform AND ma.channel = mb.channel
ORDER BY ma.actual_spend DESC;


-- Query 6: Monthly Top Wins and Key Deals
-- Largest opportunities created and closed this month
WITH month_opps AS (
    SELECT
        fp.opp_id,
        fp.stage,
        fp.amount,
        fp.product_line,
        fp.is_won,
        fp.created_date,
        fp.close_date,
        fp.win_probability,
        dc.name AS account_name,
        dc.industry,
        dc.region,
        JULIANDAY(fp.close_date) - JULIANDAY(fp.created_date) AS days_to_close
    FROM fact_pipeline fp
    LEFT JOIN dim_accounts dc ON fp.account_id = dc.account_id
    WHERE strftime('%Y-%m', fp.created_date) = strftime('%Y-%m', 'now')
       OR (fp.is_won = 1 AND strftime('%Y-%m', fp.close_date) = strftime('%Y-%m', 'now'))
)
SELECT
    opp_id,
    stage,
    ROUND(amount, 2) AS amount,
    product_line,
    is_won,
    account_name,
    industry,
    region,
    created_date,
    close_date,
    ROUND(days_to_close, 0) AS days_to_close,
    CASE WHEN is_won = 1 THEN 'Closed Won' ELSE 'In Progress' END AS status
FROM month_opps
ORDER BY amount DESC
LIMIT 10;


-- Query 7: Monthly MoM Lead Generation Summary by Source
WITH monthly_leads AS (
    SELECT
        strftime('%Y-%m', created_date) AS month_label,
        lead_source,
        COUNT(*) AS new_leads,
        SUM(CASE WHEN lead_status IN ('MQL', 'SQL', 'Opportunity', 'Customer') THEN 1 ELSE 0 END) AS mqls,
        SUM(CASE WHEN lead_status IN ('SQL', 'Opportunity', 'Customer') THEN 1 ELSE 0 END) AS sqls,
        ROUND(AVG(CASE WHEN lead_status IN ('MQL', 'SQL', 'Opportunity', 'Customer') THEN 1.0 ELSE 0.0 END) * 100, 2) AS mql_rate
    FROM dim_contacts
    WHERE created_date IS NOT NULL
    GROUP BY strftime('%Y-%m', created_date), lead_source
)
SELECT
    month_label,
    lead_source,
    new_leads,
    mqls,
    sqls,
    mql_rate,
    LAG(new_leads) OVER (PARTITION BY lead_source ORDER BY month_label) AS prev_month_leads,
    ROUND(
        (new_leads - LAG(new_leads) OVER (PARTITION BY lead_source ORDER BY month_label)) /
        NULLIF(LAG(new_leads) OVER (PARTITION BY lead_source ORDER BY month_label), 0) * 100, 2
    ) AS leads_mom_pct
FROM monthly_leads
ORDER BY month_label DESC, new_leads DESC;


-- Query 8: Monthly Web Traffic Summary with YoY
WITH monthly_web AS (
    SELECT
        strftime('%Y-%m', date) AS month_label,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNT(DISTINCT user_id) AS unique_users,
        SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) AS conversions,
        ROUND(AVG(session_duration_seconds), 0) AS avg_duration_seconds,
        ROUND(AVG(pages_per_session), 2) AS avg_pages,
        ROUND(AVG(CASE WHEN bounce = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS bounce_rate,
        ROUND(AVG(CASE WHEN engaged_session = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS engagement_rate
    FROM fact_web_sessions
    GROUP BY strftime('%Y-%m', date)
)
SELECT
    month_label,
    total_sessions,
    unique_users,
    conversions,
    ROUND(CAST(conversions AS REAL) / NULLIF(total_sessions, 0) * 100, 4) AS session_conversion_rate,
    avg_duration_seconds,
    avg_pages,
    bounce_rate,
    engagement_rate,
    LAG(total_sessions) OVER (ORDER BY month_label) AS prev_month_sessions,
    LAG(total_sessions, 12) OVER (ORDER BY month_label) AS prev_year_sessions,
    ROUND(
        (total_sessions - LAG(total_sessions) OVER (ORDER BY month_label)) /
        NULLIF(LAG(total_sessions) OVER (ORDER BY month_label), 0) * 100, 2
    ) AS sessions_mom_pct,
    ROUND(
        (total_sessions - LAG(total_sessions, 12) OVER (ORDER BY month_label)) /
        NULLIF(LAG(total_sessions, 12) OVER (ORDER BY month_label), 0) * 100, 2
    ) AS sessions_yoy_pct
FROM monthly_web
ORDER BY month_label DESC
LIMIT 13;


-- Query 9: Strategic KPI Dashboard - Monthly Summary
WITH current_month AS (SELECT strftime('%Y-%m', 'now') AS cm),
prev_month AS (SELECT strftime('%Y-%m', date('now', '-1 month')) AS pm),
kpi_current AS (
    SELECT
        'Current Month' AS period,
        SUM(ap.spend) AS total_ad_spend,
        SUM(ap.conversions) AS total_paid_conversions,
        CASE WHEN SUM(ap.conversions) > 0 THEN ROUND(SUM(ap.spend) / SUM(ap.conversions), 2) ELSE 0 END AS blended_cpa,
        CASE WHEN SUM(ap.spend) > 0 THEN ROUND(SUM(ap.conversion_value) / SUM(ap.spend), 2) ELSE 0 END AS blended_roas
    FROM fact_ad_performance ap, current_month
    WHERE strftime('%Y-%m', ap.date) = cm
),
kpi_prev AS (
    SELECT
        'Previous Month' AS period,
        SUM(ap.spend) AS total_ad_spend,
        SUM(ap.conversions) AS total_paid_conversions,
        CASE WHEN SUM(ap.conversions) > 0 THEN ROUND(SUM(ap.spend) / SUM(ap.conversions), 2) ELSE 0 END AS blended_cpa,
        CASE WHEN SUM(ap.spend) > 0 THEN ROUND(SUM(ap.conversion_value) / SUM(ap.spend), 2) ELSE 0 END AS blended_roas
    FROM fact_ad_performance ap, prev_month
    WHERE strftime('%Y-%m', ap.date) = pm
)
SELECT * FROM kpi_current
UNION ALL
SELECT * FROM kpi_prev;


-- Query 10: Monthly Data Quality Summary
SELECT
    'fact_ad_performance' AS table_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN spend IS NULL THEN 1 ELSE 0 END) AS null_spend,
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_date,
    MIN(date) AS earliest_date,
    MAX(date) AS latest_date,
    ROUND(JULIANDAY('now') - JULIANDAY(MAX(date)), 1) AS data_age_days
FROM fact_ad_performance

UNION ALL

SELECT
    'fact_web_sessions',
    COUNT(*),
    0,
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END),
    MIN(date),
    MAX(date),
    ROUND(JULIANDAY('now') - JULIANDAY(MAX(date)), 1)
FROM fact_web_sessions

UNION ALL

SELECT
    'fact_email_engagement',
    COUNT(*),
    0,
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END),
    MIN(date),
    MAX(date),
    ROUND(JULIANDAY('now') - JULIANDAY(MAX(date)), 1)
FROM fact_email_engagement

UNION ALL

SELECT
    'fact_pipeline',
    COUNT(*),
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END),
    MIN(date),
    MAX(date),
    ROUND(JULIANDAY('now') - JULIANDAY(MAX(date)), 1)
FROM fact_pipeline;
