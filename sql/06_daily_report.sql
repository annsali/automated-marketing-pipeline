-- Daily Report Queries

-- 1. Today's Spend Summary
SELECT 
    platform,
    SUM(spend) as total_spend,
    SUM(clicks) as total_clicks,
    SUM(conversions) as total_conversions,
    SUM(conversion_value) as total_revenue,
    CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
    CASE WHEN SUM(conversions) > 0 THEN SUM(spend) / SUM(conversions) ELSE 0 END as cpa,
    CASE WHEN SUM(spend) > 0 THEN SUM(conversion_value) / SUM(spend) ELSE 0 END as roas
FROM fact_ad_performance
WHERE date = date('now', '-1 day')
GROUP BY platform;

-- 2. Traffic Summary
SELECT 
    channel_group,
    COUNT(*) as sessions,
    SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) as conversions,
    AVG(session_duration_seconds) as avg_duration,
    AVG(pages_per_session) as avg_pages,
    SUM(CASE WHEN bounce = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as bounce_rate
FROM fact_web_sessions
WHERE date = date('now', '-1 day')
GROUP BY channel_group
ORDER BY sessions DESC;

-- 3. Conversion Events
SELECT 
    conversion_type,
    COUNT(*) as count
FROM fact_web_sessions
WHERE date = date('now', '-1 day')
AND converted = 1
GROUP BY conversion_type
ORDER BY count DESC;

-- 4. Email Performance
SELECT 
    COUNT(*) as sends,
    SUM(delivered) as delivered,
    SUM(opened) as opened,
    SUM(clicked) as clicked,
    CAST(SUM(delivered) AS FLOAT) / COUNT(*) as delivery_rate,
    CAST(SUM(opened) AS FLOAT) / NULLIF(SUM(delivered), 0) as open_rate,
    CAST(SUM(clicked) AS FLOAT) / NULLIF(SUM(delivered), 0) as click_rate
FROM fact_email_engagement
WHERE date = date('now', '-1 day');
