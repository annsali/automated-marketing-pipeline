"""
Web Transformer
Transforms GA4 web analytics data.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any

from .base_transformer import BaseTransformer
from config import CHANNEL_GROUPING


class WebTransformer(BaseTransformer):
    """Transformer for GA4 web analytics data."""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("web_transformer", config)
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform GA4 web data.
        
        Args:
            data: Input GA4 DataFrame
            
        Returns:
            Transformed DataFrame
        """
        rows_in = len(data)
        self.logger.info(f"Transforming {rows_in} web analytics records")
        
        df = data.copy()
        transformations = []
        
        # Filter to sessions only (not events)
        if 'record_type' in df.columns:
            df = df[df['record_type'] == 'session'].copy()
            transformations.append("filtered_to_sessions")
        
        # Assign channel grouping
        if 'source' in df.columns and 'medium' in df.columns:
            df['channel_group'] = df.apply(self._assign_channel, axis=1)
            transformations.append("assigned_channel_grouping")
        
        # Calculate engagement score
        engagement_cols = ['pages_per_session', 'session_duration_seconds']
        if all(col in df.columns for col in engagement_cols):
            df['engagement_score'] = (
                df['pages_per_session'] * 10 +
                df['session_duration_seconds'] / 10
            ) / 2
            transformations.append("calculated_engagement_score")
        
        # Flag bot traffic
        if 'session_duration_seconds' in df.columns and 'pages_per_session' in df.columns:
            df['is_bot'] = (
                (df['session_duration_seconds'] < 1) |
                (df['pages_per_session'] > 50)
            )
            bot_count = df['is_bot'].sum()
            transformations.append(f"flagged_bot_traffic: {bot_count} sessions")
        
        # Identify conversion sessions
        if 'converted' in df.columns:
            df['is_conversion_session'] = df['converted']
            transformations.append("identified_conversion_sessions")
        
        # Update metadata
        self._update_metadata(
            rows_in=rows_in,
            rows_out=len(df),
            columns_added=['channel_group', 'engagement_score', 'is_bot', 'is_conversion_session'],
            columns_removed=[],
            transformations=transformations,
        )
        
        self.logger.info(f"Web transformation complete: {len(df)} rows output")
        return df
    
    def _assign_channel(self, row: pd.Series) -> str:
        """Assign channel grouping based on source/medium."""
        source = str(row.get('source', '')).lower()
        medium = str(row.get('medium', '')).lower()
        
        # Paid Social
        if source in CHANNEL_GROUPING['paid_social']['sources'] or medium in CHANNEL_GROUPING['paid_social']['mediums']:
            return 'Paid Social'
        
        # Paid Search
        if source in CHANNEL_GROUPING['paid_search']['sources'] and medium in CHANNEL_GROUPING['paid_search']['mediums']:
            return 'Paid Search'
        
        # Organic Search
        if medium == 'organic' or source in CHANNEL_GROUPING['organic_search']['sources']:
            return 'Organic Search'
        
        # Email
        if source in CHANNEL_GROUPING['email']['sources'] or medium == 'email':
            return 'Email'
        
        # Direct
        if source in ['direct', '(direct)'] or medium in ['none', '(none)']:
            return 'Direct'
        
        # Referral
        if medium == 'referral':
            return 'Referral'
        
        # Display
        if medium in CHANNEL_GROUPING['display']['mediums']:
            return 'Display'
        
        return 'Other'
