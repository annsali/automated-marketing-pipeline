-- SQL Staging Transforms
-- Transformations applied at the staging layer

-- Query 1: Unified Ad Performance (Meta + Google)
-- Combines both ad platforms into a single schema with source_platform column

WITH meta_unified AS (
    SELECT 
        date,
        campaign_id,
        campaign_name,
        ad_set_id AS ad_group_id,
        ad_set_name AS ad_group_name,
        ad_id,
        impressions,
        clicks,
        spend AS cost,
        conversions,
        conversion_value,
        platform,
        channel,
        objective AS campaign_type,
        device,
        relevance_score AS quality_score
    FROM staging.meta_ads
),
google_unified AS (
    SELECT 
        date,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        NULL AS ad_id,
        impressions,
        clicks,
        cost,
        conversions,
        conversion_value,
        'Google' AS platform,
        CASE 
            WHEN campaign_type = 'Search' THEN 'Paid Search'
            WHEN campaign_type = 'Display' THEN 'Display'
            WHEN campaign_type = 'Video' THEN 'Video'
            ELSE 'Paid Search'
        END AS channel,
        campaign_type,
        device,
        quality_score
    FROM staging.google_ads
)
SELECT * FROM meta_unified
UNION ALL
SELECT * FROM google_unified;

-- Query 2: Channel Grouping Assignment
-- Assigns standardized channel groups to GA4 sessions

SELECT 
    session_id,
    user_id,
    date,
    landing_page,
    source,
    medium,
    campaign,
    device_category,
    country,
    city,
    session_duration_seconds,
    pages_per_session,
    bounce,
    engaged_session,
    converted,
    conversion_type,
    CASE 
        WHEN source IN ('google', 'bing') AND medium IN ('cpc', 'ppc', 'paid') THEN 'Paid Search'
        WHEN source IN ('facebook', 'instagram', 'meta', 'linkedin') OR medium IN ('social', 'paid') THEN 'Paid Social'
        WHEN medium = 'organic' OR source IN ('google', 'bing', 'yahoo') THEN 'Organic Search'
        WHEN medium = 'email' OR source IN ('email', 'sfmc') THEN 'Email'
        WHEN source IN ('direct', '(direct)') OR medium IN ('none', '(none)') THEN 'Direct'
        WHEN medium = 'referral' THEN 'Referral'
        WHEN medium IN ('display', 'banner', 'cpm') THEN 'Display'
        ELSE 'Other'
    END AS channel_group
FROM staging.ga4_sessions;

-- Query 3: Email Engagement Scoring
-- Calculates per-contact email engagement metrics

WITH contact_metrics AS (
    SELECT 
        email,
        COUNT(*) AS total_emails_sent,
        SUM(CASE WHEN delivered = 1 THEN 1 ELSE 0 END) AS total_delivered,
        SUM(CASE WHEN opened = 1 THEN 1 ELSE 0 END) AS total_opens,
        SUM(CASE WHEN clicked = 1 THEN 1 ELSE 0 END) AS total_clicks,
        SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) AS total_conversions,
        MAX(CASE WHEN opened = 1 THEN open_timestamp END) AS last_open_date,
        MAX(CASE WHEN clicked = 1 THEN click_timestamp END) AS last_click_date
    FROM staging.email_engagement
    GROUP BY email
)
SELECT 
    email,
    total_emails_sent,
    total_delivered,
    total_opens,
    total_clicks,
    total_conversions,
    CAST(total_opens AS FLOAT) / NULLIF(total_delivered, 0) AS lifetime_open_rate,
    CAST(total_clicks AS FLOAT) / NULLIF(total_delivered, 0) AS lifetime_click_rate,
    CAST(total_clicks AS FLOAT) / NULLIF(total_opens, 0) AS click_to_open_rate,
    JULIANDAY('now') - JULIANDAY(last_open_date) AS days_since_last_open,
    JULIANDAY('now') - JULIANDAY(last_click_date) AS days_since_last_click
FROM contact_metrics;

-- Query 4: CRM Data Cleaning
-- Cleans and standardizes CRM fields

SELECT 
    contact_id,
    account_id,
    email,
    name,
    title,
    department,
    -- Standardize lead status
    CASE 
        WHEN LOWER(lead_status) IN ('marketing qualified', 'marketing-qualified', 'mql') THEN 'MQL'
        WHEN LOWER(lead_status) IN ('sales qualified', 'sales-qualified', 'sql') THEN 'SQL'
        WHEN LOWER(lead_status) = 'lead' THEN 'Lead'
        WHEN LOWER(lead_status) = 'opportunity' THEN 'Opportunity'
        WHEN LOWER(lead_status) = 'customer' THEN 'Customer'
        WHEN LOWER(lead_status) = 'churned' THEN 'Churned'
        ELSE lead_status
    END AS lead_status_standardized,
    lead_source,
    created_date,
    last_activity_date,
    mql_date,
    sql_date,
    email_opt_in,
    -- Calculate derived fields
    JULIANDAY('now') - JULIANDAY(created_date) AS account_age_days,
    JULIANDAY('now') - JULIANDAY(last_activity_date) AS days_since_last_activity,
    CASE 
        WHEN mql_date IS NOT NULL AND created_date IS NOT NULL 
        THEN JULIANDAY(mql_date) - JULIANDAY(created_date)
        ELSE NULL 
    END AS lead_age_days
FROM staging.crm_contacts;
