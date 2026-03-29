-- Data Quality Validation SQL
-- Queries for data quality checks

-- Query 1: Completeness Check Across All Tables
WITH table_completeness AS (
    SELECT 
        'fact_ad_performance' as table_name,
        COUNT(*) as total_rows,
        SUM(CASE WHEN spend IS NULL THEN 1 ELSE 0 END) as null_spend,
        SUM(CASE WHEN clicks IS NULL THEN 1 ELSE 0 END) as null_clicks,
        SUM(CASE WHEN campaign_id IS NULL THEN 1 ELSE 0 END) as null_campaign
    FROM fact_ad_performance
    
    UNION ALL
    
    SELECT 
        'fact_web_sessions' as table_name,
        COUNT(*) as total_rows,
        SUM(CASE WHEN session_id IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN channel_group IS NULL THEN 1 ELSE 0 END)
    FROM fact_web_sessions
    
    UNION ALL
    
    SELECT 
        'fact_email_engagement' as table_name,
        COUNT(*) as total_rows,
        SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN campaign_id IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN delivered IS NULL THEN 1 ELSE 0 END)
    FROM fact_email_engagement
)
SELECT 
    table_name,
    total_rows,
    null_spend + null_clicks + null_campaign as total_nulls,
    ROUND(100.0 * (total_rows - (null_spend + null_clicks + null_campaign)) / total_rows, 2) as completeness_pct
FROM table_completeness;

-- Query 2: Freshness Check
SELECT 
    'fact_ad_performance' as table_name,
    MAX(date) as max_date,
    JULIANDAY('now') - JULIANDAY(MAX(date)) as days_since_update,
    CASE 
        WHEN JULIANDAY('now') - JULIANDAY(MAX(date)) <= 1 THEN 'FRESH'
        WHEN JULIANDAY('now') - JULIANDAY(MAX(date)) <= 3 THEN 'STALE'
        ELSE 'EXPIRED'
    END as freshness_status
FROM fact_ad_performance

UNION ALL

SELECT 
    'fact_web_sessions',
    MAX(date),
    JULIANDAY('now') - JULIANDAY(MAX(date)),
    CASE 
        WHEN JULIANDAY('now') - JULIANDAY(MAX(date)) <= 1 THEN 'FRESH'
        WHEN JULIANDAY('now') - JULIANDAY(MAX(date)) <= 3 THEN 'STALE'
        ELSE 'EXPIRED'
    END
FROM fact_web_sessions;

-- Query 3: Volume Anomaly Detection
WITH daily_volumes AS (
    SELECT 
        date,
        COUNT(*) as row_count
    FROM fact_ad_performance
    WHERE date >= date('now', '-30 days')
    GROUP BY date
),
stats AS (
    SELECT 
        AVG(row_count) as avg_volume,
        STDDEV(row_count) as stddev_volume
    FROM daily_volumes
)
SELECT 
    dv.date,
    dv.row_count,
    s.avg_volume,
    s.stddev_volume,
    (dv.row_count - s.avg_volume) / NULLIF(s.stddev_volume, 0) as z_score,
    CASE 
        WHEN ABS((dv.row_count - s.avg_volume) / NULLIF(s.stddev_volume, 0)) > 3 THEN 'ANOMALY'
        ELSE 'NORMAL'
    END as status
FROM daily_volumes dv
CROSS JOIN stats s
ORDER BY dv.date DESC;

-- Query 4: Duplicate Detection
SELECT 
    'fact_ad_performance' as table_name,
    date,
    campaign_id,
    COUNT(*) as duplicate_count
FROM fact_ad_performance
GROUP BY date, campaign_id
HAVING COUNT(*) > 1;
