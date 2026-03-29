"""
Tests for data transformers.
"""

import pytest
import pandas as pd
import numpy as np

from src.transformers import SchemaStandardizer, CRMTransformer, AdsTransformer


class TestSchemaStandardizer:
    """Tests for schema standardizer."""
    
    def test_to_snake_case(self):
        std = SchemaStandardizer()
        assert std.to_snake_case("CamelCase") == "camel_case"
        assert std.to_snake_case("mixedCase") == "mixed_case"
        assert std.to_snake_case("with spaces") == "with_spaces"
        assert std.to_snake_case("with-hyphens") == "with_hyphens"
    
    def test_normalize_nulls(self):
        std = SchemaStandardizer()
        assert std.normalize_nulls(None) is None
        assert std.normalize_nulls("") is None
        assert std.normalize_nulls("null") is None
        assert std.normalize_nulls("N/A") is None
        assert std.normalize_nulls("valid") == "valid"
    
    def test_transform_lowercases_columns(self):
        std = SchemaStandardizer()
        df = pd.DataFrame({"CamelCase": [1, 2], "UPPER": [3, 4]})
        
        result = std.transform(df)
        
        assert "camel_case" in result.columns
        assert "upper" in result.columns
    
    def test_transform_removes_duplicates(self):
        std = SchemaStandardizer()
        df = pd.DataFrame({"a": [1, 1], "b": [2, 2]})
        
        result = std.transform(df)
        
        assert len(result) == 1


class TestCRMTransformer:
    """Tests for CRM transformer."""
    
    def test_standardizes_lead_status(self):
        transformer = CRMTransformer()
        df = pd.DataFrame({
            "lead_status": ["Marketing Qualified", "mql", "sales-qualified", "Customer"],
            "email": ["a@test.com", "b@test.com", "c@test.com", "d@test.com"],
        })
        
        result = transformer.transform(df)
        
        assert result["lead_status"].iloc[0] == "MQL"
        assert result["lead_status"].iloc[1] == "MQL"
        assert result["lead_status"].iloc[2] == "SQL"
    
    def test_validates_email(self):
        transformer = CRMTransformer()
        df = pd.DataFrame({
            "email": ["valid@test.com", "invalid", "also@valid.com", ""],
        })
        
        result = transformer.transform(df)
        
        assert result["email_valid"].iloc[0] == True
        assert result["email_valid"].iloc[1] == False


class TestAdsTransformer:
    """Tests for Ads transformer."""
    
    def test_unifies_platforms(self):
        transformer = AdsTransformer()
        
        meta_df = pd.DataFrame({
            "date": ["2024-01-01"],
            "campaign_id": ["camp1"],
            "campaign_name": ["Test Meta"],
            "impressions": [1000],
            "clicks": [50],
            "spend": [100],
            "conversions": [5],
            "conversion_value": [500],
            "objective": ["CONVERSIONS"],
        })
        
        google_df = pd.DataFrame({
            "date": ["2024-01-01"],
            "campaign_id": ["camp2"],
            "campaign_name": ["Test Google"],
            "impressions": [2000],
            "clicks": [100],
            "cost": [200],
            "conversions": [10],
            "conversion_value": [1000],
            "campaign_type": ["Search"],
            "ad_group_id": ["ag1"],
        })
        
        result = transformer.transform(meta_df, google_df)
        
        assert len(result) == 2
        assert "Meta" in result["platform"].values
        assert "Google" in result["platform"].values
    
    def test_calculates_derived_metrics(self):
        transformer = AdsTransformer()
        
        meta_df = pd.DataFrame({
            "date": ["2024-01-01"],
            "campaign_id": ["camp1"],
            "campaign_name": ["Test"],
            "impressions": [1000],
            "clicks": [50],
            "spend": [100],
            "conversions": [5],
            "conversion_value": [500],
            "objective": ["CONVERSIONS"],
        })
        
        result = transformer.transform(meta_df, None)
        
        assert "ctr" in result.columns
        assert "cpc" in result.columns
        assert "cpa" in result.columns
        assert "roas" in result.columns
        
        # Check calculations
        assert abs(result["ctr"].iloc[0] - 0.05) < 0.001  # 50/1000
        assert abs(result["cpc"].iloc[0] - 2.0) < 0.001   # 100/50
