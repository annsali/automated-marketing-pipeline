"""
Anomaly Detector
Statistical anomaly detection for marketing metrics.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List
from scipy import stats

from config import ANOMALY_CONFIG


class AnomalyDetector:
    """Statistical anomaly detection for time series metrics."""
    
    def __init__(self, config: Dict = None):
        self.config = config or ANOMALY_CONFIG
        self.anomalies: List[Dict[str, Any]] = []
    
    def detect_zscore(
        self,
        data: pd.DataFrame,
        metric_column: str,
        date_column: str = 'date',
        groupby_column: str = None
    ) -> List[Dict[str, Any]]:
        """
        Detect anomalies using Z-score method.
        
        Args:
            data: Time series DataFrame
            metric_column: Column containing metric values
            date_column: Date column
            groupby_column: Optional column to group by (e.g., platform)
            
        Returns:
            List of anomaly records
        """
        anomalies = []
        
        df = data.copy()
        df[date_column] = pd.to_datetime(df[date_column])
        df = df.sort_values(date_column)
        
        groups = [None] if groupby_column is None else df[groupby_column].unique()
        
        for group in groups:
            if group is None:
                subset = df
            else:
                subset = df[df[groupby_column] == group]
            
            if len(subset) < self.config['min_data_points']:
                continue
            
            # Calculate rolling statistics
            subset = subset.sort_values(date_column)
            values = subset[metric_column].values
            
            # Use trailing window for mean/std
            window = min(self.config['trailing_days'], len(values) - 1)
            
            for i in range(window, len(values)):
                current_value = values[i]
                trailing_values = values[i-window:i]
                
                if len(trailing_values) < 2:
                    continue
                
                mean = np.mean(trailing_values)
                std = np.std(trailing_values)
                
                if std == 0:
                    # If std=0 but current value differs from mean, it's a definite anomaly
                    if current_value != mean:
                        z_score = float('inf') if current_value > mean else float('-inf')
                    else:
                        continue
                else:
                    z_score = (current_value - mean) / std
                
                if abs(z_score) > self.config['z_score_threshold']:
                    pct_change = ((current_value - mean) / mean) if mean != 0 else 0
                    
                    anomaly = {
                        "method": "z_score",
                        "metric": f"{groupby_column}_{group}_{metric_column}" if group else metric_column,
                        "date": subset.iloc[i][date_column].strftime("%Y-%m-%d"),
                        "observed_value": round(current_value, 2),
                        "expected_value": round(mean, 2),
                        "z_score": round(z_score, 2),
                        "pct_change": round(pct_change, 4),
                        "severity": "CRITICAL" if abs(z_score) > 4 else "WARNING",
                        "likely_cause": self._infer_cause(metric_column, pct_change),
                        "recommendation": self._get_recommendation(metric_column, pct_change),
                    }
                    anomalies.append(anomaly)
        
        return anomalies
    
    def detect_iqr(
        self,
        data: pd.DataFrame,
        metric_column: str,
        date_column: str = 'date',
        groupby_column: str = None
    ) -> List[Dict[str, Any]]:
        """Detect anomalies using IQR method."""
        anomalies = []
        
        df = data.copy()
        df[date_column] = pd.to_datetime(df[date_column])
        
        groups = [None] if groupby_column is None else df[groupby_column].unique()
        
        for group in groups:
            if group is None:
                subset = df
            else:
                subset = df[df[groupby_column] == group]
            
            if len(subset) < self.config['min_data_points']:
                continue
            
            values = subset[metric_column].values
            q1 = np.percentile(values, 25)
            q3 = np.percentile(values, 75)
            iqr = q3 - q1
            
            lower_bound = q1 - self.config['iqr_multiplier'] * iqr
            upper_bound = q3 + self.config['iqr_multiplier'] * iqr
            
            for idx, row in subset.iterrows():
                value = row[metric_column]
                if value < lower_bound or value > upper_bound:
                    pct_diff = ((value - np.median(values)) / np.median(values)) if np.median(values) != 0 else 0
                    
                    anomaly = {
                        "method": "iqr",
                        "metric": f"{groupby_column}_{group}_{metric_column}" if group else metric_column,
                        "date": row[date_column].strftime("%Y-%m-%d"),
                        "observed_value": round(value, 2),
                        "expected_range": [round(lower_bound, 2), round(upper_bound, 2)],
                        "iqr": round(iqr, 2),
                        "pct_diff": round(pct_diff, 4),
                        "severity": "WARNING",
                        "likely_cause": "Value outside normal range",
                        "recommendation": "Review for data entry errors or genuine anomalies",
                    }
                    anomalies.append(anomaly)
        
        return anomalies
    
    def detect_pct_change(
        self,
        data: pd.DataFrame,
        metric_column: str,
        date_column: str = 'date',
        groupby_column: str = None
    ) -> List[Dict[str, Any]]:
        """Detect anomalies based on day-over-day percentage change."""
        anomalies = []
        
        df = data.copy()
        df[date_column] = pd.to_datetime(df[date_column])
        df = df.sort_values(date_column)
        
        groups = [None] if groupby_column is None else df[groupby_column].unique()
        
        for group in groups:
            if group is None:
                subset = df
            else:
                subset = df[df[groupby_column] == group].copy()
            
            if len(subset) < 2:
                continue
            
            subset['pct_change'] = subset[metric_column].pct_change()
            
            for idx, row in subset.iterrows():
                pct_change = abs(row['pct_change']) if pd.notna(row['pct_change']) else 0
                
                if pct_change > self.config['pct_change_threshold']:
                    anomaly = {
                        "method": "pct_change",
                        "metric": f"{groupby_column}_{group}_{metric_column}" if group else metric_column,
                        "date": row[date_column].strftime("%Y-%m-%d"),
                        "observed_value": round(row[metric_column], 2),
                        "pct_change": round(row['pct_change'], 4),
                        "threshold": self.config['pct_change_threshold'],
                        "severity": "CRITICAL" if pct_change > 0.5 else "WARNING",
                        "likely_cause": "Sudden significant change in metric",
                        "recommendation": "Investigate recent changes to campaigns or tracking",
                    }
                    anomalies.append(anomaly)
        
        return anomalies
    
    def detect_all(
        self,
        data: pd.DataFrame,
        metrics: List[str],
        date_column: str = 'date',
        groupby_column: str = None
    ) -> List[Dict[str, Any]]:
        """
        Run all anomaly detection methods.
        
        Args:
            data: Time series DataFrame
            metrics: List of metric columns to check
            date_column: Date column
            groupby_column: Optional grouping column
            
        Returns:
            Combined list of all anomalies
        """
        all_anomalies = []
        
        for metric in metrics:
            if metric not in data.columns:
                continue
            
            # Z-score detection
            zscore_anomalies = self.detect_zscore(data, metric, date_column, groupby_column)
            all_anomalies.extend(zscore_anomalies)
            
            # IQR detection
            iqr_anomalies = self.detect_iqr(data, metric, date_column, groupby_column)
            all_anomalies.extend(iqr_anomalies)
            
            # Percentage change detection
            pct_anomalies = self.detect_pct_change(data, metric, date_column, groupby_column)
            all_anomalies.extend(pct_anomalies)
        
        # Deduplicate anomalies (same metric, date, method)
        seen = set()
        unique_anomalies = []
        for a in all_anomalies:
            key = (a['metric'], a['date'], a['method'])
            if key not in seen:
                seen.add(key)
                unique_anomalies.append(a)
        
        self.anomalies = unique_anomalies
        return unique_anomalies
    
    def _infer_cause(self, metric: str, pct_change: float) -> str:
        """Infer likely cause based on metric and change direction."""
        if 'spend' in metric.lower():
            return "Campaign paused or budget depleted" if pct_change < 0 else "Budget increase or new campaigns"
        elif 'conversion' in metric.lower():
            return "Tracking issue or campaign fatigue" if pct_change < 0 else "New campaigns or seasonality"
        elif 'ctr' in metric.lower():
            return "Creative fatigue or targeting issue" if pct_change < 0 else "New creative or improved targeting"
        elif 'delivery' in metric.lower():
            return "Deliverability issue" if pct_change < 0 else "List quality improvement"
        return "Unknown cause - investigate"
    
    def _get_recommendation(self, metric: str, pct_change: float) -> str:
        """Get recommendation based on metric and change."""
        if 'spend' in metric.lower():
            return "Verify campaign status and budget settings"
        elif 'conversion' in metric.lower():
            return "Check tracking pixels and conversion events"
        elif 'ctr' in metric.lower():
            return "Review creative performance and refresh ads"
        elif 'delivery' in metric.lower():
            return "Check sender reputation and list hygiene"
        return "Review data and investigate further"
