"""
Monthly Report
Monthly Executive Report generation.
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from .report_engine import ReportEngine


class MonthlyReport:
    """Generator for monthly executive reports."""
    
    def __init__(self, engine: ReportEngine = None):
        self.engine = engine or ReportEngine()
    
    def generate(self, report_date: datetime = None) -> Dict[str, Any]:
        """Generate the monthly executive report."""
        report_date = report_date or datetime.now()
        
        # Current month
        month_start = report_date.replace(day=1)
        prev_month_start = (month_start - timedelta(days=1)).replace(day=1)
        
        queries = {
            "monthly_summary": f"""
                SELECT 
                    SUM(spend) as total_spend,
                    SUM(clicks) as total_clicks,
                    SUM(conversions) as total_conversions,
                    SUM(conversion_value) as total_revenue
                FROM fact_ad_performance
                WHERE date >= '{month_start.strftime("%Y-%m-%d")}'
            """,
            "channel_attribution": f"""
                SELECT 
                    channel,
                    SUM(spend) as spend,
                    SUM(conversion_value) as revenue,
                    CASE WHEN SUM(spend) > 0 THEN SUM(conversion_value) / SUM(spend) ELSE 0 END as roas
                FROM fact_ad_performance
                WHERE date >= '{month_start.strftime("%Y-%m-%d")}'
                GROUP BY channel
                ORDER BY revenue DESC
            """,
            "pipeline_forecast": f"""
                SELECT 
                    stage,
                    COUNT(*) as count,
                    SUM(amount) as pipeline_value
                FROM fact_pipeline
                WHERE is_won = 0 AND close_date > '{month_start.strftime("%Y-%m-%d")}'
                GROUP BY stage
            """,
            "won_deals": f"""
                SELECT 
                    opp_id,
                    amount,
                    product_line
                FROM fact_pipeline
                WHERE is_won = 1 AND close_date >= '{month_start.strftime("%Y-%m-%d")}'
                ORDER BY amount DESC
                LIMIT 5
            """,
        }
        
        template = """===================================================
MONTHLY EXECUTIVE REPORT - {{ report_date }}
===================================================

EXECUTIVE SUMMARY
{% if monthly_summary %}
{% for row in monthly_summary %}
  Total Spend:            ${{ "%.0f"|format(row.total_spend) }}
  Total Clicks:           {{ "%.0f"|format(row.total_clicks) }}
  Total Conversions:      {{ "%.0f"|format(row.total_conversions) }}
  Total Revenue:          ${{ "%.0f"|format(row.total_revenue) }}
  Blended ROAS:           {{ "%.1f"|format(row.total_revenue/row.total_spend if row.total_spend > 0 else 0) }}x
{% endfor %}
{% endif %}

CHANNEL ATTRIBUTION
{% if channel_attribution %}
| Channel | Spend | Revenue | ROAS |
|---------|-------|---------|------|
{% for row in channel_attribution %}
| {{ row.channel }} | ${{ "%.0f"|format(row.spend) }} | ${{ "%.0f"|format(row.revenue) }} | {{ "%.1f"|format(row.roas) }}x |
{% endfor %}
{% endif %}

PIPELINE FORECAST
{% if pipeline_forecast %}
{% for row in pipeline_forecast %}
  {{ row.stage }}: {{ row.count }} opps, ${{ "%.0f"|format(row.pipeline_value) }}
{% endfor %}
{% endif %}

TOP WON DEALS THIS MONTH
{% if won_deals %}
{% for row in won_deals %}
  {{ loop.index }}. {{ row.opp_id }}: ${{ "%.0f"|format(row.amount) }} ({{ row.product_line }})
{% endfor %}
{% endif %}

===================================================
"""
        
        return self.engine.generate_report(
            queries=queries,
            template=template,
            report_type="monthly",
            report_date=report_date
        )
