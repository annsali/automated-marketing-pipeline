-- =============================================================================
-- Weekly Report SQL Queries
-- Marketing Data Pipeline - Weekly Performance Summary
-- =============================================================================

-- Query 1: Weekly Spend Summary with Week-over-Week Comparison
-- Uses CTEs + LAG window function for WoW comparison
WITH weekly_spend AS (
    SELECT
        strftime('%Y-W%W', date) AS week_label,
        MIN(date) AS week_start,
        MAX(date) AS week_end,
        platform,
        SUM(spend) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(conversions) AS total_conversions,
        SUM(conversion_value) AS total_conversion_value,
        CASE WHEN SUM(clicks) > 0 THEN ROUND(SUM(spend) / SUM(clicks), 2) ELSE 0 END AS avg_cpc,
        CASE WHEN SUM(impressions) > 0 THEN ROUND(CAST(SUM(clicks) AS REAL) / SUM(impressions) * 100, 4) ELSE 0 END AS avg_ctr,
        CASE WHEN SUM(conversions) > 0 THEN ROUND(SUM(spend) / SUM(conversions), 2) ELSE 0 END AS avg_cpa,
        CASE WHEN SUM(spend) > 0 THEN ROUND(SUM(conversion_value) / SUM(spend), 2) ELSE 0 END AS roas
    FROM fact_ad_performance
    GROUP BY strftime('%Y-W%W', date), platform
),
weekly_totals AS (
    SELECT
        week_label,
        week_start,
        week_end,
        SUM(total_spend) AS total_spend,
        SUM(total_impressions) AS total_impressions,
        SUM(total_clicks) AS total_clicks,
        SUM(total_conversions) AS total_conversions,
        SUM(total_conversion_value) AS total_conversion_value
    FROM weekly_spend
    GROUP BY week_label, week_start, week_end
),
wow_comparison AS (
    SELECT
        w1.week_label,
        w1.week_start,
        w1.week_end,
        w1.total_spend,
        w1.total_impressions,
        w1.total_clicks,
        w1.total_conversions,
        w1.total_conversion_value,
        LAG(w1.total_spend) OVER (ORDER BY w1.week_start) AS prev_week_spend,
        LAG(w1.total_conversions) OVER (ORDER BY w1.week_start) AS prev_week_conversions,
        LAG(w1.total_clicks) OVER (ORDER BY w1.week_start) AS prev_week_clicks,
        ROUND(
            (w1.total_spend - LAG(w1.total_spend) OVER (ORDER BY w1.week_start)) /
            NULLIF(LAG(w1.total_spend) OVER (ORDER BY w1.week_start), 0) * 100, 2
        ) AS spend_wow_pct,
        ROUND(
            (w1.total_conversions - LAG(w1.total_conversions) OVER (ORDER BY w1.week_start)) /
            NULLIF(LAG(w1.total_conversions) OVER (ORDER BY w1.week_start), 0) * 100, 2
        ) AS conversions_wow_pct
    FROM weekly_totals w1
)
SELECT
    week_label,
    week_start,
    week_end,
    ROUND(total_spend, 2) AS total_spend,
    total_impressions,
    total_clicks,
    total_conversions,
    ROUND(total_conversion_value, 2) AS total_conversion_value,
    ROUND(prev_week_spend, 2) AS prev_week_spend,
    spend_wow_pct,
    conversions_wow_pct
FROM wow_comparison
ORDER BY week_start DESC
LIMIT 8;


-- Query 2: Weekly Channel Performance Breakdown
-- Uses CTEs + aggregation for channel-level WoW comparison
WITH channel_weekly AS (
    SELECT
        strftime('%Y-W%W', s.date) AS week_label,
        MIN(s.date) AS week_start,
        s.channel_group AS channel,
        COUNT(DISTINCT s.session_id) AS sessions,
        SUM(CASE WHEN s.converted = 1 THEN 1 ELSE 0 END) AS conversions,
        ROUND(AVG(s.session_duration_seconds), 0) AS avg_session_duration,
        ROUND(AVG(CASE WHEN s.bounce = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS bounce_rate
    FROM fact_web_sessions s
    GROUP BY strftime('%Y-W%W', s.date), s.channel_group
),
channel_ads_weekly AS (
    SELECT
        strftime('%Y-W%W', date) AS week_label,
        channel,
        SUM(spend) AS spend,
        SUM(clicks) AS clicks,
        SUM(conversions) AS paid_conversions,
        CASE WHEN SUM(conversions) > 0 THEN ROUND(SUM(spend) / SUM(conversions), 2) ELSE 0 END AS cpa,
        CASE WHEN SUM(spend) > 0 THEN ROUND(SUM(conversion_value) / SUM(spend), 2) ELSE 0 END AS roas
    FROM fact_ad_performance
    GROUP BY strftime('%Y-W%W', date), channel
)
SELECT
    cw.week_label,
    cw.week_start,
    cw.channel,
    cw.sessions,
    cw.conversions,
    cw.avg_session_duration,
    cw.bounce_rate,
    COALESCE(ca.spend, 0) AS spend,
    COALESCE(ca.cpa, 0) AS cpa,
    COALESCE(ca.roas, 0) AS roas
FROM channel_weekly cw
LEFT JOIN channel_ads_weekly ca
    ON cw.week_label = ca.week_label
    AND LOWER(cw.channel) = LOWER(ca.channel)
ORDER BY cw.week_start DESC, cw.sessions DESC;


-- Query 3: Top 5 Campaigns by Conversions This Week
-- Uses CTEs + RANK window function
WITH current_week AS (
    SELECT
        strftime('%Y-W%W', 'now') AS current_week_label
),
campaign_weekly_perf AS (
    SELECT
        strftime('%Y-W%W', ap.date) AS week_label,
        ap.campaign_id,
        ap.campaign_name,
        ap.platform,
        ap.channel,
        SUM(ap.spend) AS total_spend,
        SUM(ap.impressions) AS total_impressions,
        SUM(ap.clicks) AS total_clicks,
        SUM(ap.conversions) AS total_conversions,
        SUM(ap.conversion_value) AS total_conversion_value,
        CASE WHEN SUM(ap.conversions) > 0 THEN ROUND(SUM(ap.spend) / SUM(ap.conversions), 2) ELSE 0 END AS cpa,
        CASE WHEN SUM(ap.spend) > 0 THEN ROUND(SUM(ap.conversion_value) / SUM(ap.spend), 2) ELSE 0 END AS roas,
        CASE WHEN SUM(ap.clicks) > 0 THEN ROUND(CAST(SUM(ap.conversions) AS REAL) / SUM(ap.clicks) * 100, 4) ELSE 0 END AS conversion_rate
    FROM fact_ad_performance ap
    GROUP BY strftime('%Y-W%W', ap.date), ap.campaign_id, ap.campaign_name, ap.platform, ap.channel
),
ranked_campaigns AS (
    SELECT
        cwp.*,
        RANK() OVER (PARTITION BY week_label ORDER BY total_conversions DESC) AS conversion_rank,
        RANK() OVER (PARTITION BY week_label ORDER BY roas DESC) AS efficiency_rank
    FROM campaign_weekly_perf cwp
)
SELECT
    campaign_id,
    campaign_name,
    platform,
    channel,
    ROUND(total_spend, 2) AS total_spend,
    total_impressions,
    total_clicks,
    total_conversions,
    cpa,
    roas,
    conversion_rate,
    conversion_rank,
    efficiency_rank
FROM ranked_campaigns
WHERE week_label = (SELECT current_week_label FROM current_week)
ORDER BY conversion_rank
LIMIT 10;


-- Query 4: Bottom 5 Campaigns (Pause Candidates)
-- High spend, low conversions, poor efficiency
WITH current_week AS (
    SELECT strftime('%Y-W%W', 'now') AS current_week_label
),
campaign_efficiency AS (
    SELECT
        strftime('%Y-W%W', date) AS week_label,
        campaign_id,
        campaign_name,
        platform,
        SUM(spend) AS total_spend,
        SUM(conversions) AS total_conversions,
        SUM(impressions) AS total_impressions,
        CASE WHEN SUM(conversions) > 0 THEN ROUND(SUM(spend) / SUM(conversions), 2) ELSE 99999 END AS cpa,
        CASE WHEN SUM(spend) > 0 THEN ROUND(SUM(conversion_value) / SUM(spend), 2) ELSE 0 END AS roas,
        CASE WHEN SUM(impressions) > 0 THEN ROUND(CAST(SUM(clicks) AS REAL) / SUM(impressions) * 100, 4) ELSE 0 END AS ctr
    FROM fact_ad_performance
    WHERE spend > 0
    GROUP BY strftime('%Y-W%W', date), campaign_id, campaign_name, platform
)
SELECT
    campaign_id,
    campaign_name,
    platform,
    ROUND(total_spend, 2) AS total_spend,
    total_conversions,
    cpa,
    roas,
    ctr,
    'Pause Candidate' AS recommendation
FROM campaign_efficiency
WHERE week_label = (SELECT current_week_label FROM current_week)
    AND total_spend > 50
    AND (total_conversions = 0 OR cpa > 500 OR roas < 0.5)
ORDER BY total_spend DESC
LIMIT 5;


-- Query 5: Weekly CRM Funnel Snapshot
-- MQLs -> SQLs -> Opps -> Closed Won this week
WITH date_range AS (
    SELECT
        date('now', '-6 days') AS week_start,
        date('now') AS week_end
)
SELECT
    'MQLs Generated' AS funnel_stage,
    COUNT(*) AS count,
    1 AS sort_order
FROM dim_contacts, date_range
WHERE mql_date BETWEEN week_start AND week_end

UNION ALL

SELECT
    'SQLs Generated' AS funnel_stage,
    COUNT(*) AS count,
    2
FROM dim_contacts, date_range
WHERE sql_date BETWEEN week_start AND week_end

UNION ALL

SELECT
    'Opportunities Created' AS funnel_stage,
    COUNT(*) AS count,
    3
FROM fact_pipeline, date_range
WHERE created_date BETWEEN week_start AND week_end

UNION ALL

SELECT
    'Closed Won' AS funnel_stage,
    COUNT(*) AS count,
    4
FROM fact_pipeline, date_range
WHERE is_won = 1
    AND close_date BETWEEN week_start AND week_end

ORDER BY sort_order;


-- Query 6: Lead Velocity Rate (LVR) - Weekly Trend
-- New qualified leads per week over last 12 weeks
WITH weekly_leads AS (
    SELECT
        strftime('%Y-W%W', mql_date) AS week_label,
        MIN(mql_date) AS week_start,
        COUNT(*) AS new_mqls
    FROM dim_contacts
    WHERE mql_date IS NOT NULL
    GROUP BY strftime('%Y-W%W', mql_date)
    ORDER BY week_start DESC
    LIMIT 12
)
SELECT
    week_label,
    week_start,
    new_mqls,
    LAG(new_mqls) OVER (ORDER BY week_start) AS prev_week_mqls,
    ROUND(
        (CAST(new_mqls AS REAL) - LAG(new_mqls) OVER (ORDER BY week_start)) /
        NULLIF(LAG(new_mqls) OVER (ORDER BY week_start), 0) * 100, 2
    ) AS lvr_wow_pct,
    ROUND(AVG(new_mqls) OVER (ORDER BY week_start ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 1) AS rolling_4wk_avg
FROM weekly_leads
ORDER BY week_start DESC;


-- Query 7: Weekly Email Performance Summary
WITH weekly_email AS (
    SELECT
        strftime('%Y-W%W', date) AS week_label,
        MIN(date) AS week_start,
        COUNT(*) AS total_sends,
        SUM(delivered) AS total_delivered,
        SUM(opened) AS total_opened,
        SUM(clicked) AS total_clicked,
        SUM(bounced) AS total_bounced,
        SUM(unsubscribed) AS total_unsubscribed,
        SUM(converted) AS total_converted
    FROM fact_email_engagement
    GROUP BY strftime('%Y-W%W', date)
)
SELECT
    week_label,
    week_start,
    total_sends,
    total_delivered,
    total_opened,
    total_clicked,
    ROUND(CAST(total_delivered AS REAL) / NULLIF(total_sends, 0) * 100, 2) AS delivery_rate,
    ROUND(CAST(total_opened AS REAL) / NULLIF(total_delivered, 0) * 100, 2) AS open_rate,
    ROUND(CAST(total_clicked AS REAL) / NULLIF(total_delivered, 0) * 100, 2) AS click_rate,
    ROUND(CAST(total_clicked AS REAL) / NULLIF(total_opened, 0) * 100, 2) AS ctor,
    ROUND(CAST(total_bounced AS REAL) / NULLIF(total_sends, 0) * 100, 2) AS bounce_rate,
    ROUND(CAST(total_unsubscribed AS REAL) / NULLIF(total_delivered, 0) * 100, 2) AS unsub_rate,
    LAG(total_sends) OVER (ORDER BY week_start) AS prev_week_sends,
    ROUND(
        (CAST(total_sends AS REAL) - LAG(total_sends) OVER (ORDER BY week_start)) /
        NULLIF(LAG(total_sends) OVER (ORDER BY week_start), 0) * 100, 2
    ) AS sends_wow_pct
FROM weekly_email
ORDER BY week_start DESC
LIMIT 8;


-- Query 8: Budget Shift Recommendations
-- Compares channel efficiency to suggest reallocation
WITH channel_perf AS (
    SELECT
        channel,
        SUM(spend) AS total_spend,
        SUM(conversions) AS total_conversions,
        CASE WHEN SUM(conversions) > 0 THEN ROUND(SUM(spend) / SUM(conversions), 2) ELSE 99999 END AS cpa,
        CASE WHEN SUM(spend) > 0 THEN ROUND(SUM(conversion_value) / SUM(spend), 2) ELSE 0 END AS roas,
        CASE WHEN SUM(impressions) > 0 THEN ROUND(CAST(SUM(clicks) AS REAL) / SUM(impressions) * 100, 4) ELSE 0 END AS avg_ctr
    FROM fact_ad_performance
    WHERE date >= date('now', '-7 days')
    GROUP BY channel
),
avg_metrics AS (
    SELECT
        AVG(cpa) AS avg_cpa,
        AVG(roas) AS avg_roas
    FROM channel_perf
    WHERE total_spend > 0
)
SELECT
    cp.channel,
    ROUND(cp.total_spend, 2) AS total_spend,
    cp.total_conversions,
    cp.cpa,
    cp.roas,
    cp.avg_ctr,
    CASE
        WHEN cp.roas > am.avg_roas * 1.2 AND cp.cpa < am.avg_cpa * 0.8
            THEN 'INCREASE BUDGET - High efficiency channel'
        WHEN cp.roas < am.avg_roas * 0.8 AND cp.total_spend > 100
            THEN 'DECREASE BUDGET - Below average efficiency'
        WHEN cp.total_conversions = 0 AND cp.total_spend > 50
            THEN 'PAUSE - No conversions this week'
        ELSE 'MAINTAIN - On track'
    END AS recommendation
FROM channel_perf cp, avg_metrics am
ORDER BY cp.roas DESC;
