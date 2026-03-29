-- Identity Resolution SQL
-- Cross-platform identity matching queries

-- Create identity graph from exact email matches
INSERT INTO staging.identity_graph (
    master_id, 
    crm_contact_id, 
    crm_email,
    ga4_user_id,
    email_recipient_id,
    match_confidence,
    match_method
)
SELECT 
    'MASTER' || printf('%08d', ROW_NUMBER() OVER (ORDER BY c.email)),
    c.contact_id,
    c.email,
    NULL, -- Would match GA4 users if email was available
    e.recipient_id,
    1.0,
    'exact_email'
FROM staging.crm_contacts c
LEFT JOIN (
    SELECT DISTINCT email, recipient_id 
    FROM staging.email_engagement
) e ON LOWER(c.email) = LOWER(e.email)
WHERE c.email IS NOT NULL;

-- Match GA4 sessions via UTM campaign matching
UPDATE staging.identity_graph
SET ga4_user_id = (
    SELECT MIN(s.user_id)
    FROM staging.web_sessions s
    WHERE s.campaign LIKE '%' || (
        SELECT SUBSTRING(campaign_name, 1, 20) 
        FROM staging.unified_ads ua
        WHERE ua.campaign_id = staging.identity_graph.crm_contact_id
    ) || '%'
    LIMIT 1
)
WHERE ga4_user_id IS NULL;

-- Calculate match statistics
SELECT 
    'CRM Contacts' as source,
    COUNT(*) as total_records,
    SUM(CASE WHEN ga4_user_id IS NOT NULL THEN 1 ELSE 0 END) as matched_to_ga4,
    SUM(CASE WHEN email_recipient_id IS NOT NULL THEN 1 ELSE 0 END) as matched_to_email,
    ROUND(
        CAST(SUM(CASE WHEN ga4_user_id IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100, 
        1
    ) as ga4_match_rate_pct
FROM staging.identity_graph;

-- Unmatched records for review
SELECT 
    master_id,
    crm_contact_id,
    crm_email,
    match_confidence,
    'No GA4 match' as issue
FROM staging.identity_graph
WHERE ga4_user_id IS NULL AND email_recipient_id IS NULL;
