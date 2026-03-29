-- SQL Warehouse Models
-- DDL and queries for the unified data warehouse

-- Dimension Tables

-- Date dimension population
INSERT OR IGNORE INTO dim_dates (
    date, day_of_week, week_number, month, quarter, year, 
    is_weekend, is_holiday, fiscal_quarter
)
WITH RECURSIVE dates(date) AS (
    SELECT date('2023-01-01')
    UNION ALL
    SELECT date(date, '+1 day')
    FROM dates
    WHERE date < date('2026-12-31')
)
SELECT 
    date,
    CAST(strftime('%w', date) AS INTEGER) as day_of_week,
    CAST(strftime('%W', date) AS INTEGER) as week_number,
    CAST(strftime('%m', date) AS INTEGER) as month,
    (CAST(strftime('%m', date) AS INTEGER) + 2) / 3 as quarter,
    CAST(strftime('%Y', date) AS INTEGER) as year,
    CASE WHEN CAST(strftime('%w', date) AS INTEGER) IN (0, 6) THEN 1 ELSE 0 END as is_weekend,
    0 as is_holiday, -- Would need holiday calendar
    (CAST(strftime('%m', date) AS INTEGER) + 2) / 3 as fiscal_quarter
FROM dates;

-- Fact Table Insert Queries

-- Insert Ad Performance
INSERT INTO fact_ad_performance (
    date, platform, campaign_id, ad_group_id, ad_id, channel,
    impressions, clicks, spend, conversions, conversion_value,
    ctr, cpc, cpa, roas, device, quality_score, loaded_at
)
SELECT 
    date,
    platform,
    campaign_id,
    ad_group_id,
    ad_id,
    channel,
    impressions,
    clicks,
    spend,
    conversions,
    conversion_value,
    CASE WHEN impressions > 0 THEN CAST(clicks AS FLOAT) / impressions ELSE 0 END as ctr,
    CASE WHEN clicks > 0 THEN spend / clicks ELSE 0 END as cpc,
    CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END as cpa,
    CASE WHEN spend > 0 THEN conversion_value / spend ELSE 0 END as roas,
    device,
    quality_score,
    datetime('now')
FROM staging.unified_ads;

-- Insert Web Sessions
INSERT INTO fact_web_sessions (
    date, session_id, user_id, master_id, channel, channel_group,
    landing_page, source, medium, campaign, device_category,
    country, city, session_duration_seconds, pages_per_session,
    bounce, engaged_session, converted, conversion_type,
    engagement_score, is_bot, loaded_at
)
SELECT 
    s.date,
    s.session_id,
    s.user_id,
    ig.master_id,
    s.channel,
    s.channel_group,
    s.landing_page,
    s.source,
    s.medium,
    s.campaign,
    s.device_category,
    s.country,
    s.city,
    s.session_duration_seconds,
    s.pages_per_session,
    s.bounce,
    s.engaged_session,
    s.converted,
    s.conversion_type,
    (s.pages_per_session * 10 + s.session_duration_seconds / 10.0) / 2 as engagement_score,
    s.is_bot,
    datetime('now')
FROM staging.web_sessions s
LEFT JOIN staging.identity_graph ig ON s.user_id = ig.ga4_user_id;

-- Insert Email Engagement
INSERT INTO fact_email_engagement (
    date, campaign_id, recipient_id, master_id, email,
    delivered, opened, clicked, bounced, unsubscribed, converted,
    open_timestamp, click_timestamp, conversion_timestamp, loaded_at
)
SELECT 
    date(e.sent_at) as date,
    e.campaign_id,
    e.recipient_id,
    ig.master_id,
    e.email,
    e.delivered,
    e.opened,
    e.clicked,
    e.bounced,
    e.unsubscribed,
    e.converted,
    e.open_timestamp,
    e.click_timestamp,
    e.conversion_timestamp,
    datetime('now')
FROM staging.email_engagement e
LEFT JOIN staging.identity_graph ig ON e.email = ig.crm_email;

-- Materialized View: Campaign Performance Summary
CREATE TABLE IF NOT EXISTS mv_campaign_performance AS
SELECT 
    fap.campaign_id,
    dc.campaign_name,
    fap.platform,
    fap.channel,
    MIN(fap.date) as first_date,
    MAX(fap.date) as last_date,
    SUM(fap.impressions) as total_impressions,
    SUM(fap.clicks) as total_clicks,
    SUM(fap.spend) as total_spend,
    SUM(fap.conversions) as total_conversions,
    SUM(fap.conversion_value) as total_revenue,
    AVG(fap.ctr) as avg_ctr,
    AVG(fap.roas) as avg_roas,
    CASE WHEN SUM(fap.spend) > 0 THEN SUM(fap.conversion_value) / SUM(fap.spend) ELSE 0 END as overall_roas
FROM fact_ad_performance fap
LEFT JOIN dim_campaigns dc ON fap.campaign_id = dc.campaign_id
GROUP BY fap.campaign_id, fap.platform, fap.channel;

-- Index for performance
CREATE INDEX IF NOT EXISTS idx_mv_campaign ON mv_campaign_performance(campaign_id);
