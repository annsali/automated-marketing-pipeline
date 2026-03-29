"""
Report Engine
Core report generation engine.
"""

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from jinja2 import Template

from config import WAREHOUSE_DB, REPORTS_DIR


class ReportEngine:
    """Core engine for generating reports from warehouse data."""
    
    def __init__(self, db_path: Path = None, output_dir: Path = None):
        self.db_path = db_path or WAREHOUSE_DB
        self.output_dir = output_dir or REPORTS_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query against warehouse."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    
    def render_template(self, template_str: str, data: Dict[str, Any]) -> str:
        """Render a Jinja2 template with data."""
        template = Template(template_str)
        return template.render(**data)
    
    def generate_report(
        self,
        queries: Dict[str, str],
        template: str,
        report_type: str,
        report_date: datetime = None
    ) -> Dict[str, Any]:
        """
        Generate a report from queries and template.
        
        Args:
            queries: Dict of query_name -> SQL query
            template: Jinja2 template string
            report_type: Type of report (daily, weekly, monthly)
            report_date: Date for the report
            
        Returns:
            Dict with report content and metadata
        """
        report_date = report_date or datetime.now()
        date_str = report_date.strftime("%Y-%m-%d")
        
        # Execute all queries
        query_results = {}
        for name, query in queries.items():
            try:
                query_results[name] = self.execute_query(query)
            except Exception as e:
                query_results[name] = pd.DataFrame()
                print(f"Query '{name}' failed: {e}")
        
        # Convert DataFrames to dicts for template
        template_data = {
            "report_date": date_str,
            "report_type": report_type,
            "generated_at": datetime.now().isoformat(),
        }
        
        for name, df in query_results.items():
            template_data[name] = df.to_dict('records')
            template_data[f"{name}_df"] = df
        
        # Render template
        report_content = self.render_template(template, template_data)
        
        # Save report
        filename = f"{report_type}_report_{report_date.strftime('%Y%m%d')}.md"
        filepath = self.output_dir / filename
        
        with open(filepath, "w") as f:
            f.write(report_content)
        
        return {
            "report_type": report_type,
            "report_date": date_str,
            "filepath": str(filepath),
            "content": report_content,
            "queries_executed": len(queries),
            "generated_at": datetime.now().isoformat(),
        }
    
    def get_metric(self, metric_name: str, query: str) -> Any:
        """Get a single metric value from the warehouse."""
        df = self.execute_query(query)
        if not df.empty and len(df.columns) > 0:
            return df.iloc[0, 0]
        return None
    
    def format_currency(self, value: float) -> str:
        """Format value as currency."""
        if value is None:
            return "$0"
        if value >= 1_000_000:
            return f"${value/1_000_000:.1f}M"
        elif value >= 1_000:
            return f"${value/1_000:.1f}K"
        return f"${value:,.0f}"
    
    def format_number(self, value: int) -> str:
        """Format large numbers."""
        if value is None:
            return "0"
        if value >= 1_000_000:
            return f"{value/1_000_000:.1f}M"
        elif value >= 1_000:
            return f"{value/1_000:.1f}K"
        return f"{value:,}"
    
    def format_pct(self, value: float) -> str:
        """Format as percentage."""
        if value is None:
            return "0%"
        return f"{value:.1%}"
