"""
Daily Report
Daily Marketing Pulse report generation.
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from .report_engine import ReportEngine


class DailyReport:
    """Generator for daily marketing reports."""
    
    def __init__(self, engine: ReportEngine = None):
        self.engine = engine or ReportEngine()
    
    def generate(self, report_date: datetime = None) -> Dict[str, Any]:
        """Generate the daily marketing pulse report."""
        report_date = report_date or datetime.now()
        yesterday = report_date - timedelta(days=1)
        week_ago = report_date - timedelta(days=7)
        
        date_str = yesterday.strftime("%Y-%m-%d")
        
        queries = {
            "spend_today": f"""
                SELECT 
                    platform,
                    SUM(spend) as spend,
                    SUM(clicks) as clicks,
                    SUM(conversions) as conversions,
                    SUM(conversion_value) as revenue
                FROM fact_ad_performance
                WHERE date = '{date_str}'
                GROUP BY platform
            """,
            "traffic_today": f"""
                SELECT 
                    channel_group,
                    COUNT(*) as sessions,
                    SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) as conversions
                FROM fact_web_sessions
                WHERE date = '{date_str}'
                GROUP BY channel_group
                ORDER BY sessions DESC
            """,
            "email_today": f"""
                SELECT 
                    COUNT(*) as sends,
                    SUM(delivered) as delivered,
                    SUM(opened) as opened,
                    SUM(clicked) as clicked
                FROM fact_email_engagement
                WHERE date = '{date_str}'
            """,
        }
        
        template = """===================================================
DAILY MARKETING PULSE - {{ report_date }}
Data as of: {{ generated_at }} | Pipeline Status: [HEALTHY]
===================================================

SPEND & EFFICIENCY
{% if spend_today %}
{% for row in spend_today %}
  {{ row.platform }} Ads: Spend ${{ "{:.0f}".format(row.spend|default(0)) }}, Clicks {{ row.clicks|default(0) }}, Conv {{ row.conversions|default(0) }}
{% endfor %}
{% else %}
  No spend data available
{% endif %}

TRAFFIC & ENGAGEMENT
{% if traffic_today %}
{% for row in traffic_today %}
  {{ row.channel_group }}: {{ row.sessions|default(0) }} sessions
{% endfor %}
{% else %}
  No traffic data available
{% endif %}

EMAIL PERFORMANCE
{% if email_today %}
{% for row in email_today %}
  Sends Today: {{ row.sends|default(0) }}
  Delivered: {{ row.delivered|default(0) }}
  Opened: {{ row.opened|default(0) }}
  Clicked: {{ row.clicked|default(0) }}
{% endfor %}
{% else %}
  No email data available
{% endif %}

===================================================
"""
        
        return self.engine.generate_report(
            queries=queries,
            template=template,
            report_type="daily",
            report_date=report_date
        )
