"""
Data Quality Checks
Individual data quality check implementations.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List

from config import DQ_THRESHOLDS


class DQChecks:
    """Individual data quality check implementations."""
    
    def __init__(self):
        self.thresholds = DQ_THRESHOLDS
    
    def check_completeness(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Check percentage of NULL values per column."""
        null_counts = df.isnull().sum()
        total_rows = len(df)
        
        completeness = {}
        low_completeness = []
        
        for col in df.columns:
            pct_complete = 1 - (null_counts[col] / total_rows) if total_rows > 0 else 1
            completeness[col] = pct_complete
            
            if pct_complete < self.thresholds['completeness_min']:
                low_completeness.append(f"{col} ({pct_complete:.1%})")
        
        overall_completeness = np.mean(list(completeness.values()))
        
        return {
            "check_name": "check_completeness",
            "table": table_name,
            "status": "PASS" if overall_completeness >= self.thresholds['completeness_min'] else "FAIL",
            "severity": "CRITICAL",
            "metric_value": overall_completeness,
            "threshold": self.thresholds['completeness_min'],
            "details": f"Low completeness columns: {', '.join(low_completeness)}" if low_completeness else "All columns meet threshold",
            "affected_rows": sum(null_counts),
            "total_rows": total_rows,
        }
    
    def check_uniqueness(self, df: pd.DataFrame, table_name: str, key_columns: List[str]) -> Dict[str, Any]:
        """Check for duplicate primary keys."""
        if not key_columns or not all(col in df.columns for col in key_columns):
            return {
                "check_name": "check_uniqueness",
                "table": table_name,
                "status": "WARNING",
                "severity": "CRITICAL",
                "metric_value": 0,
                "threshold": 1.0,
                "details": f"Key columns {key_columns} not found",
                "affected_rows": 0,
                "total_rows": len(df),
            }
        
        total_rows = len(df)
        unique_rows = len(df.drop_duplicates(subset=key_columns))
        duplicate_rows = total_rows - unique_rows
        uniqueness = unique_rows / total_rows if total_rows > 0 else 1
        
        return {
            "check_name": "check_uniqueness",
            "table": table_name,
            "status": "PASS" if uniqueness >= self.thresholds['uniqueness_min'] else "FAIL",
            "severity": "CRITICAL",
            "metric_value": uniqueness,
            "threshold": self.thresholds['uniqueness_min'],
            "details": f"{duplicate_rows} duplicate rows found" if duplicate_rows > 0 else "All rows unique",
            "affected_rows": duplicate_rows,
            "total_rows": total_rows,
        }
    
    def check_freshness(self, df: pd.DataFrame, table_name: str, date_column: str) -> Dict[str, Any]:
        """Check if data is fresh (recent records present)."""
        if date_column not in df.columns:
            return {
                "check_name": "check_freshness",
                "table": table_name,
                "status": "WARNING",
                "severity": "HIGH",
                "metric_value": 0,
                "threshold": self.thresholds['freshness_hours'],
                "details": f"Date column '{date_column}' not found",
                "affected_rows": 0,
                "total_rows": len(df),
            }
        
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        max_date = df[date_column].max()
        
        if pd.isna(max_date):
            return {
                "check_name": "check_freshness",
                "table": table_name,
                "status": "FAIL",
                "severity": "HIGH",
                "metric_value": float('inf'),
                "threshold": self.thresholds['freshness_hours'],
                "details": "No valid dates found",
                "affected_rows": len(df),
                "total_rows": len(df),
            }
        
        hours_since_update = (datetime.now() - max_date).total_seconds() / 3600
        
        return {
            "check_name": "check_freshness",
            "table": table_name,
            "status": "PASS" if hours_since_update <= self.thresholds['freshness_hours'] else "FAIL",
            "severity": "HIGH",
            "metric_value": hours_since_update,
            "threshold": self.thresholds['freshness_hours'],
            "details": f"Last update {hours_since_update:.1f} hours ago (max: {self.thresholds['freshness_hours']}h)",
            "affected_rows": 0,
            "total_rows": len(df),
        }
    
    def check_volume(self, df: pd.DataFrame, table_name: str, expected_rows: int = None) -> Dict[str, Any]:
        """Check if row count is within expected range."""
        actual_rows = len(df)
        
        if expected_rows is None:
            # Use a default expectation based on table
            expected_rows = 1000
        
        tolerance = self.thresholds['volume_tolerance']
        lower_bound = expected_rows * (1 - tolerance)
        upper_bound = expected_rows * (1 + tolerance)
        
        within_range = lower_bound <= actual_rows <= upper_bound
        
        return {
            "check_name": "check_volume",
            "table": table_name,
            "status": "PASS" if within_range else "WARNING",
            "severity": "HIGH",
            "metric_value": actual_rows,
            "threshold": expected_rows,
            "details": f"Rows: {actual_rows}, Expected: {expected_rows} (±{tolerance:.0%})",
            "affected_rows": 0,
            "total_rows": actual_rows,
        }
    
    def check_schema(self, df: pd.DataFrame, table_name: str, expected_columns: List[str]) -> Dict[str, Any]:
        """Check if expected columns are present."""
        actual_columns = set(df.columns)
        expected_set = set(expected_columns)
        
        missing = expected_set - actual_columns
        extra = actual_columns - expected_set
        
        return {
            "check_name": "check_schema",
            "table": table_name,
            "status": "PASS" if not missing else "FAIL",
            "severity": "CRITICAL",
            "metric_value": len(expected_set - missing) / len(expected_set) if expected_set else 1,
            "threshold": 1.0,
            "details": f"Missing: {list(missing)}, Extra: {list(extra)}",
            "affected_rows": 0,
            "total_rows": len(df),
        }
    
    def check_value_range(self, df: pd.DataFrame, table_name: str, column: str, min_val: float = None, max_val: float = None) -> Dict[str, Any]:
        """Check if numeric values are within expected range."""
        if column not in df.columns:
            return {
                "check_name": "check_value_range",
                "table": table_name,
                "status": "WARNING",
                "severity": "MEDIUM",
                "metric_value": 0,
                "threshold": 1.0,
                "details": f"Column '{column}' not found",
                "affected_rows": 0,
                "total_rows": len(df),
            }
        
        col_data = df[column].dropna()
        
        out_of_range = 0
        if min_val is not None:
            out_of_range += (col_data < min_val).sum()
        if max_val is not None:
            out_of_range += (col_data > max_val).sum()
        
        pct_valid = 1 - (out_of_range / len(col_data)) if len(col_data) > 0 else 1
        
        return {
            "check_name": "check_value_range",
            "table": table_name,
            "status": "PASS" if out_of_range == 0 else "WARNING",
            "severity": "MEDIUM",
            "metric_value": pct_valid,
            "threshold": 1.0,
            "details": f"{out_of_range} values out of range [{min_val}, {max_val}]",
            "affected_rows": int(out_of_range),
            "total_rows": len(df),
        }
    
    def check_email_validity(self, df: pd.DataFrame, table_name: str, email_column: str = 'email') -> Dict[str, Any]:
        """Check email format validity."""
        import re
        
        if email_column not in df.columns:
            return {
                "check_name": "check_email_validity",
                "table": table_name,
                "status": "WARNING",
                "severity": "LOW",
                "metric_value": 0,
                "threshold": self.thresholds['email_validity_min'],
                "details": f"Email column '{email_column}' not found",
                "affected_rows": 0,
                "total_rows": len(df),
            }
        
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        emails = df[email_column].dropna()
        valid_emails = emails.apply(lambda x: bool(re.match(pattern, str(x))) if pd.notna(x) else False)
        
        pct_valid = valid_emails.mean() if len(emails) > 0 else 1
        invalid_count = (~valid_emails).sum()
        
        return {
            "check_name": "check_email_validity",
            "table": table_name,
            "status": "PASS" if pct_valid >= self.thresholds['email_validity_min'] else "WARNING",
            "severity": "LOW",
            "metric_value": pct_valid,
            "threshold": self.thresholds['email_validity_min'],
            "details": f"{invalid_count} invalid emails out of {len(emails)}",
            "affected_rows": int(invalid_count),
            "total_rows": len(df),
        }
