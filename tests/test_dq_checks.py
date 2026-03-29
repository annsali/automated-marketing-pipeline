"""
Tests for data quality checks.
"""

import pytest
import pandas as pd
import numpy as np

from src.quality import DQChecks


class TestDQChecks:
    """Tests for DQ check implementations."""
    
    @pytest.fixture
    def dq_checks(self):
        return DQChecks()
    
    def test_check_completeness_pass(self, dq_checks):
        df = pd.DataFrame({
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        })
        
        result = dq_checks.check_completeness(df, "test_table")
        
        assert result["status"] == "PASS"
        assert result["metric_value"] == 1.0
    
    def test_check_completeness_fail(self, dq_checks):
        df = pd.DataFrame({
            "a": [1, None, 3],
            "b": [4, 5, None],
        })
        
        result = dq_checks.check_completeness(df, "test_table")
        
        assert result["status"] == "FAIL"
        assert result["metric_value"] < 1.0
    
    def test_check_uniqueness_pass(self, dq_checks):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        })
        
        result = dq_checks.check_uniqueness(df, "test_table", ["id"])
        
        assert result["status"] == "PASS"
        assert result["metric_value"] == 1.0
    
    def test_check_uniqueness_fail(self, dq_checks):
        df = pd.DataFrame({
            "id": [1, 2, 2],
            "value": ["a", "b", "c"],
        })
        
        result = dq_checks.check_uniqueness(df, "test_table", ["id"])
        
        assert result["status"] == "FAIL"
        assert result["affected_rows"] == 1
    
    def test_check_volume(self, dq_checks):
        df = pd.DataFrame({"a": range(100)})
        
        result = dq_checks.check_volume(df, "test_table", expected_rows=100)
        
        assert result["status"] == "PASS"
    
    def test_check_email_validity(self, dq_checks):
        df = pd.DataFrame({
            "email": ["valid@test.com", "invalid", "also@valid.com"],
        })
        
        result = dq_checks.check_email_validity(df, "test_table")
        
        assert result["status"] == "WARNING"
        assert result["affected_rows"] == 1
