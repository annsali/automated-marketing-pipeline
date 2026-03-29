"""
Data Quality Engine
Core validation orchestrator for data quality checks.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd

from .dq_checks import DQChecks


class DQEngine:
    """Core data quality validation engine."""
    
    def __init__(self):
        self.logger = logging.getLogger("dq_engine")
        self.checks = DQChecks()
        self.check_results: List[Dict[str, Any]] = []
        self.overall_score: float = 0.0
        self.status: str = "UNKNOWN"  # PASS, WARNING, FAIL
    
    def run_checks(self, tables: Dict[str, pd.DataFrame], table_configs: Dict[str, Dict] = None) -> Dict[str, Any]:
        """
        Run all data quality checks on provided tables.
        
        Args:
            tables: Dict of table_name -> DataFrame
            table_configs: Configuration for each table (key columns, date columns, etc.)
            
        Returns:
            DQ report dict
        """
        start_time = time.time()
        self.check_results = []
        
        table_configs = table_configs or {}
        
        for table_name, df in tables.items():
            if df.empty:
                self.logger.warning(f"Table {table_name} is empty, skipping checks")
                continue
            
            config = table_configs.get(table_name, {})
            self.logger.info(f"Running DQ checks on {table_name} ({len(df)} rows)")
            
            # Run completeness check
            result = self.checks.check_completeness(df, table_name)
            self._add_result(result)
            
            # Run uniqueness check
            key_cols = config.get('key_columns', [df.columns[0]])
            result = self.checks.check_uniqueness(df, table_name, key_cols)
            self._add_result(result)
            
            # Run freshness check
            date_col = config.get('date_column')
            if date_col:
                result = self.checks.check_freshness(df, table_name, date_col)
                self._add_result(result)
            
            # Run volume check
            expected = config.get('expected_rows')
            result = self.checks.check_volume(df, table_name, expected)
            self._add_result(result)
            
            # Run schema check
            expected_cols = config.get('expected_columns', list(df.columns))
            result = self.checks.check_schema(df, table_name, expected_cols)
            self._add_result(result)
            
            # Run value range checks for numeric columns
            range_checks = config.get('range_checks', [])
            for check in range_checks:
                result = self.checks.check_value_range(
                    df, table_name, 
                    check['column'], 
                    check.get('min'), 
                    check.get('max')
                )
                self._add_result(result)
            
            # Run email validity check
            if 'email' in df.columns or config.get('has_email'):
                email_col = config.get('email_column', 'email')
                result = self.checks.check_email_validity(df, table_name, email_col)
                self._add_result(result)
        
        # Calculate overall score and status
        self._calculate_score()
        
        execution_time = time.time() - start_time
        
        report = {
            "dq_score": round(self.overall_score, 1),
            "status": self.status,
            "checks_run": len(self.check_results),
            "checks_passed": sum(1 for r in self.check_results if r['status'] == 'PASS'),
            "checks_warning": sum(1 for r in self.check_results if r['status'] == 'WARNING'),
            "checks_failed": sum(1 for r in self.check_results if r['status'] == 'FAIL'),
            "execution_time_seconds": round(execution_time, 2),
            "executed_at": datetime.utcnow().isoformat(),
            "results": self.check_results,
        }
        
        self.logger.info(f"DQ Engine complete: Score {report['dq_score']}/100, Status: {report['status']}")
        
        return report
    
    def _add_result(self, result: Dict[str, Any]):
        """Add a check result with execution timestamp."""
        result['executed_at'] = datetime.utcnow().isoformat()
        self.check_results.append(result)
    
    def _calculate_score(self):
        """Calculate overall DQ score and status."""
        if not self.check_results:
            self.overall_score = 0
            self.status = "FAIL"
            return
        
        # Weight by severity
        weights = {
            "CRITICAL": 3,
            "HIGH": 2,
            "MEDIUM": 1,
            "LOW": 0.5,
        }
        
        total_weight = 0
        weighted_score = 0
        
        has_critical_fail = False
        
        for result in self.check_results:
            weight = weights.get(result['severity'], 1)
            total_weight += weight
            
            if result['status'] == 'PASS':
                weighted_score += weight * 100
            elif result['status'] == 'WARNING':
                weighted_score += weight * 70
            else:  # FAIL
                weighted_score += weight * 0
                if result['severity'] == 'CRITICAL':
                    has_critical_fail = True
        
        self.overall_score = weighted_score / total_weight if total_weight > 0 else 0
        
        # Determine status
        if has_critical_fail or self.overall_score < 70:
            self.status = "FAIL"
        elif self.overall_score < 90:
            self.status = "WARNING"
        else:
            self.status = "PASS"
    
    def should_halt_pipeline(self) -> bool:
        """Check if pipeline should halt due to DQ failures."""
        return self.status == "FAIL"
