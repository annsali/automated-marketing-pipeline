"""
Tests for anomaly detection.
"""

import pytest
import pandas as pd
import numpy as np

from src.quality import AnomalyDetector


class TestAnomalyDetector:
    """Tests for anomaly detection."""
    
    @pytest.fixture
    def detector(self):
        return AnomalyDetector()
    
    def test_detect_zscore_finds_anomaly(self, detector):
        # Create data with one clear outlier
        dates = pd.date_range("2024-01-01", periods=30)
        values = [100] * 29 + [500]  # Last value is 5x normal
        
        df = pd.DataFrame({
            "date": dates,
            "spend": values,
        })
        
        anomalies = detector.detect_zscore(df, "spend")
        
        assert len(anomalies) > 0
        assert any(a["metric"].endswith("spend") for a in anomalies)
    
    def test_detect_zscore_no_anomaly_normal_data(self, detector):
        # Create normally distributed data
        np.random.seed(42)
        dates = pd.date_range("2024-01-01", periods=30)
        values = np.random.normal(100, 10, 30)  # Small variance
        
        df = pd.DataFrame({
            "date": dates,
            "spend": values,
        })
        
        anomalies = detector.detect_zscore(df, "spend")
        
        # Should find few or no anomalies in normal data
        assert len(anomalies) < 3
    
    def test_detect_pct_change(self, detector):
        # Create data with sudden drop
        dates = pd.date_range("2024-01-01", periods=10)
        values = [100] * 5 + [50] * 5  # 50% drop
        
        df = pd.DataFrame({
            "date": dates,
            "clicks": values,
        })
        
        anomalies = detector.detect_pct_change(df, "clicks")
        
        # Should detect the change
        assert len(anomalies) > 0
