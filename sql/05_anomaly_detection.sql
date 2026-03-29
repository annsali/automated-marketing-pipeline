-- Anomaly Detection SQL
-- Statistical anomaly detection queries

-- Query 5: Metric Anomaly Detection (Z-Score)
WITH daily_metrics AS (
    SELECT 
        date,
        platform,
        SUM(spend) as daily_spend,
        SUM(clicks) as daily_clicks,
        AVG(ctr) as avg_ctr
    FROM fact_ad_performance
    WHERE date >= date('now', '-60 days')
    GROUP BY date, platform
),
stats AS (
    SELECT 
        platform,
        AVG(daily_spend) as avg_spend,
        STDDEV(daily_spend) as stddev_spend,
        AVG(daily_clicks) as avg_clicks,
        STDDEV(daily_clicks) as stddev_clicks
    FROM daily_metrics
    GROUP BY platform
)
SELECT 
    dm.date,
    dm.platform,
    dm.daily_spend,
    s.avg_spend,
    (dm.daily_spend - s.avg_spend) / NULLIF(s.stddev_spend, 0) as spend_zscore,
    CASE 
        WHEN ABS((dm.daily_spend - s.avg_spend) / NULLIF(s.stddev_spend, 0)) > 3 THEN 'ANOMALY'
        ELSE 'NORMAL'
    END as spend_status,
    dm.daily_clicks,
    (dm.daily_clicks - s.avg_clicks) / NULLIF(s.stddev_clicks, 0) as clicks_zscore
FROM daily_metrics dm
JOIN stats s ON dm.platform = s.platform
WHERE ABS((dm.daily_spend - s.avg_spend) / NULLIF(s.stddev_spend, 0)) > 2
ORDER BY dm.date DESC, dm.platform;

-- Query 6: CTR Drop Detection
WITH campaign_ctrs AS (
    SELECT 
        campaign_id,
        date,
        ctr,
        LAG(ctr) OVER (PARTITION BY campaign_id ORDER BY date) as prev_ctr,
        AVG(ctr) OVER (
            PARTITION BY campaign_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) as trailing_avg_ctr
    FROM fact_ad_performance
    WHERE date >= date('now', '-30 days')
)
SELECT 
    campaign_id,
    date,
    ctr,
    prev_ctr,
    trailing_avg_ctr,
    CASE 
        WHEN prev_ctr > 0 THEN (ctr - prev_ctr) / prev_ctr 
        ELSE 0 
    END as pct_change,
    CASE 
        WHEN ctr < trailing_avg_ctr * 0.5 THEN 'SIGNIFICANT_DROP'
        WHEN ctr = 0 AND prev_ctr > 0 THEN 'ZERO_CTR'
        ELSE 'OK'
    END as alert_type
FROM campaign_ctrs
WHERE date >= date('now', '-7 days')
AND (
    (ctr < trailing_avg_ctr * 0.5 AND trailing_avg_ctr > 0)
    OR (ctr = 0 AND prev_ctr > 0)
)
ORDER BY date DESC, pct_change ASC;
