"""
Tests for data extractors.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta

from src.extractors import CRMExtractor, MetaAdsExtractor, GoogleAdsExtractor, GA4Extractor, EmailPlatformExtractor


class TestCRMExtractor:
    """Tests for CRM extractor."""
    
    def test_extract_returns_dataframe(self):
        extractor = CRMExtractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        df = extractor.extract(start, end)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
    
    def test_validate_response_passes(self):
        extractor = CRMExtractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        df = extractor.extract(start, end)
        assert extractor.validate_response(df) is True
    
    def test_validate_response_fails_empty(self):
        extractor = CRMExtractor()
        assert extractor.validate_response(pd.DataFrame()) is False
    
    def test_metadata_populated(self):
        extractor = CRMExtractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        extractor.extract(start, end)
        metadata = extractor.get_metadata()
        
        assert 'source' in metadata
        assert 'extracted_at' in metadata
        assert metadata['source'] == 'crm'


class TestMetaAdsExtractor:
    """Tests for Meta Ads extractor."""
    
    def test_extract_returns_dataframe(self):
        extractor = MetaAdsExtractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        df = extractor.extract(start, end)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert 'campaign_id' in df.columns
        assert 'spend' in df.columns
    
    def test_no_negative_spend(self):
        extractor = MetaAdsExtractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        df = extractor.extract(start, end)
        
        assert (df['spend'] >= 0).all()
    
    def test_validate_response(self):
        extractor = MetaAdsExtractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        df = extractor.extract(start, end)
        assert extractor.validate_response(df) is True


class TestGoogleAdsExtractor:
    """Tests for Google Ads extractor."""
    
    def test_extract_returns_dataframe(self):
        extractor = GoogleAdsExtractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        df = extractor.extract(start, end)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
    
    def test_cost_conversion(self):
        extractor = GoogleAdsExtractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        df = extractor.extract(start, end)
        
        # Cost should be in dollars (not micros)
        assert (df['cost'] < 10000).all()  # Reasonable spend


class TestGA4Extractor:
    """Tests for GA4 extractor."""
    
    def test_extract_returns_dataframe(self):
        extractor = GA4Extractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        df = extractor.extract(start, end)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0


class TestEmailExtractor:
    """Tests for Email extractor."""
    
    def test_extract_returns_dataframe(self):
        extractor = EmailPlatformExtractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        df = extractor.extract(start, end)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert 'email' in df.columns
    
    def test_realistic_rates(self):
        extractor = EmailPlatformExtractor()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        df = extractor.extract(start, end)
        
        delivery_rate = df['delivered'].sum() / len(df)
        open_rate = df['opened'].sum() / df['delivered'].sum() if df['delivered'].sum() > 0 else 0
        
        # Check realistic ranges
        assert 0.90 < delivery_rate < 1.0
        assert 0.10 < open_rate < 0.50
