"""
Weekly Report
Weekly Performance Summary report generation.
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from .report_engine import ReportEngine


class WeeklyReport:
    """Generator for weekly marketing reports."""
    
    def __init__(self, engine: ReportEngine = None):
        self.engine = engine or ReportEngine()
    
    def generate(self, report_date: datetime = None) -> Dict[str, Any]:
        """Generate the weekly performance summary report."""
        report_date = report_date or datetime.now()
        
        # Get current week and previous week
        week_start = report_date - timedelta(days=report_date.weekday() + 7)
        week_end = week_start + timedelta(days=6)
        prev_week_start = week_start - timedelta(days=7)
        prev_week_end = prev_week_start + timedelta(days=6)
        
        queries = {
            "channel_performance": f"""
                SELECT 
                    channel,
                    SUM(spend) as spend,
                    SUM(clicks) as clicks,
                    SUM(conversions) as conversions,
                    SUM(conversion_value) as revenue,
                    CASE WHEN SUM(spend) > 0 THEN SUM(conversion_value) / SUM(spend) ELSE 0 END as roas,
                    CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
                    CASE WHEN SUM(conversions) > 0 THEN SUM(spend) / SUM(conversions) ELSE 0 END as cpa
                FROM fact_ad_performance
                WHERE date BETWEEN '{week_start.strftime("%Y-%m-%d")}' AND '{week_end.strftime("%Y-%m-%d")}'
                GROUP BY channel
                ORDER BY spend DESC
            """,
            "top_campaigns": f"""
                SELECT 
                    campaign_name,
                    platform,
                    SUM(spend) as spend,
                    SUM(conversions) as conversions,
                    CASE WHEN SUM(spend) > 0 THEN SUM(conversion_value) / SUM(spend) ELSE 0 END as roas
                FROM fact_ad_performance
                WHERE date BETWEEN '{week_start.strftime("%Y-%m-%d")}' AND '{week_end.strftime("%Y-%m-%d")}'
                GROUP BY campaign_id
                ORDER BY conversions DESC
                LIMIT 5
            """,
            "funnel": f"""
                SELECT 
                    'MQLs' as stage,
                    COUNT(*) as count
                FROM dim_contacts
                WHERE mql_date BETWEEN '{week_start.strftime("%Y-%m-%d")}' AND '{week_end.strftime("%Y-%m-%d")}'
                UNION ALL
                SELECT 
                    'Opportunities' as stage,
                    COUNT(*)
                FROM fact_pipeline
                WHERE created_date BETWEEN '{week_start.strftime("%Y-%m-%d")}' AND '{week_end.strftime("%Y-%m-%d")}'
                UNION ALL
                SELECT 
                    'Closed Won' as stage,
                    COUNT(*)
                FROM fact_pipeline
                WHERE close_date BETWEEN '{week_start.strftime("%Y-%m-%d")}' AND '{week_end.strftime("%Y-%m-%d")}' AND is_won = 1
            """,
        }
        
        template = """===================================================
WEEKLY MARKETING SUMMARY - Week of {{ report_date }}
Data Period: {{ report_date }}
===================================================

CHANNEL PERFORMANCE
{% if channel_performance %}
| Channel | Spend | Clicks | Conv | ROAS | CPA |
|---------|-------|--------|------|------|-----|
{% for row in channel_performance %}
| {{ row.channel }} | ${{ "%.0f"|format(row.spend) }} | {{ "%.0f"|format(row.clicks) }} | {{ row.conversions }} | {{ "%.1f"|format(row.roas) }}x | ${{ "%.0f"|format(row.cpa) }} |
{% endfor %}
{% else %}
No channel performance data available.
{% endif %}

TOP 5 CAMPAIGNS BY CONVERSIONS
{% if top_campaigns %}
{% for row in top_campaigns %}
  {{ loop.index }}. {{ row.campaign_name }} ({{ row.platform }})
     Spend: ${{ "%.0f"|format(row.spend) }} | Conv: {{ row.conversions }} | ROAS: {{ "%.1f"|format(row.roas) }}x
{% endfor %}
{% else %}
No campaign data available.
{% endif %}

FUNNEL SNAPSHOT
{% if funnel %}
{% for row in funnel %}
  {{ row.stage }}: {{ row.count }}
{% endfor %}
{% else %}
No funnel data available.
{% endif %}

===================================================
"""
        
        return self.engine.generate_report(
            queries=queries,
            template=template,
            report_type="weekly",
            report_date=report_date
        )
