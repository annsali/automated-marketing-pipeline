"""
Ads Transformer
Unifies Meta Ads and Google Ads data.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple

from .base_transformer import BaseTransformer


class AdsTransformer(BaseTransformer):
    """Transformer for unifying ad platform data."""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("ads_transformer", config)
    
    def transform(
        self, 
        meta_data: pd.DataFrame = None, 
        google_data: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        Transform and unify Meta Ads and Google Ads data.
        
        Args:
            meta_data: Meta Ads DataFrame
            google_data: Google Ads DataFrame
            
        Returns:
            Unified DataFrame
        """
        unified_frames = []
        transformations = []
        
        if meta_data is not None and not meta_data.empty:
            meta_transformed = self._transform_meta(meta_data)
            unified_frames.append(meta_transformed)
            transformations.append("transformed_meta_ads")
        
        if google_data is not None and not google_data.empty:
            google_transformed = self._transform_google(google_data)
            unified_frames.append(google_transformed)
            transformations.append("transformed_google_ads")
        
        if not unified_frames:
            return pd.DataFrame()
        
        # Combine unified data
        unified = pd.concat(unified_frames, ignore_index=True)
        
        # Standardize campaign naming
        unified = self._standardize_campaign_names(unified)
        transformations.append("standardized_campaign_names")
        
        # Calculate derived metrics
        unified = self._calculate_derived_metrics(unified)
        transformations.append("calculated_derived_metrics")
        
        # Flag anomalous days
        unified = self._flag_anomalies(unified)
        transformations.append("flagged_anomalous_days")
        
        rows_in = (len(meta_data) if meta_data is not None else 0) + (len(google_data) if google_data is not None else 0)
        
        self._update_metadata(
            rows_in=rows_in,
            rows_out=len(unified),
            columns_added=['channel', 'ctr', 'cpc', 'cpa', 'roas', 'anomaly_flag'],
            columns_removed=[],
            transformations=transformations,
        )
        
        self.logger.info(f"Ads transformation complete: {len(unified)} unified rows")
        return unified
    
    def _transform_meta(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform Meta Ads data to unified schema."""
        df = data.copy()
        
        # Map Meta fields to unified schema
        unified = pd.DataFrame({
            'date': df['date'],
            'platform': 'Meta',
            'platform_id': df['campaign_id'],
            'campaign_id': df['campaign_id'],
            'campaign_name': df['campaign_name'],
            'ad_group_id': df.get('ad_set_id'),
            'ad_group_name': df.get('ad_set_name'),
            'ad_id': df.get('ad_id'),
            'impressions': df['impressions'],
            'clicks': df['clicks'],
            'spend': df['spend'],
            'conversions': df['conversions'],
            'conversion_value': df['conversion_value'],
            'objective': df.get('objective'),
            'channel': self._map_meta_objective(df.get('objective')),
            'device': None,
            'quality_score': df.get('relevance_score'),
        })
        
        return unified
    
    def _transform_google(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform Google Ads data to unified schema."""
        df = data.copy()
        
        # Map Google fields to unified schema
        unified = pd.DataFrame({
            'date': df['date'],
            'platform': 'Google',
            'platform_id': df['campaign_id'],
            'campaign_id': df['campaign_id'],
            'campaign_name': df['campaign_name'],
            'ad_group_id': df.get('ad_group_id'),
            'ad_group_name': df.get('ad_group_name'),
            'ad_id': None,
            'impressions': df['impressions'],
            'clicks': df['clicks'],
            'spend': df['cost'],
            'conversions': df['conversions'],
            'conversion_value': df['conversion_value'],
            'objective': df.get('campaign_type'),
            'channel': self._map_google_objective(df.get('campaign_type')),
            'device': df.get('device'),
            'quality_score': df.get('quality_score'),
        })
        
        return unified
    
    def _map_meta_objective(self, objectives: pd.Series) -> pd.Series:
        """Map Meta objective to channel."""
        mapping = {
            'CONVERSIONS': 'Paid Social',
            'TRAFFIC': 'Paid Social',
            'LEAD_GENERATION': 'Paid Social',
            'AWARENESS': 'Paid Social',
        }
        return objectives.map(mapping).fillna('Paid Social')
    
    def _map_google_objective(self, campaign_types: pd.Series) -> pd.Series:
        """Map Google campaign type to channel."""
        mapping = {
            'Search': 'Paid Search',
            'Display': 'Display',
            'Video': 'Video',
            'Performance_Max': 'Paid Search',
        }
        return campaign_types.map(mapping).fillna('Paid Search')
    
    def _standardize_campaign_names(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract components from campaign names."""
        df = data.copy()
        
        # Extract campaign type
        df['campaign_type_extracted'] = df['campaign_name'].str.extract(r'(Search|Social|Display|Video)')[0]
        
        # Extract quarter
        df['quarter'] = df['campaign_name'].str.extract(r'Q([1-4])')[0]
        
        # Extract year
        df['year'] = df['campaign_name'].str.extract(r'202([0-9])')[0]
        
        return df
    
    def _calculate_derived_metrics(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate derived metrics."""
        df = data.copy()
        
        # CTR
        df['ctr'] = np.where(df['impressions'] > 0, df['clicks'] / df['impressions'], 0)
        
        # CPC
        df['cpc'] = np.where(df['clicks'] > 0, df['spend'] / df['clicks'], 0)
        
        # CPA
        df['cpa'] = np.where(df['conversions'] > 0, df['spend'] / df['conversions'], 0)
        
        # ROAS
        df['roas'] = np.where(df['spend'] > 0, df['conversion_value'] / df['spend'], 0)
        
        return df
    
    def _flag_anomalies(self, data: pd.DataFrame) -> pd.DataFrame:
        """Flag anomalous days in the data."""
        df = data.copy()
        
        # Calculate daily averages by campaign
        campaign_stats = df.groupby('campaign_id').agg({
            'spend': 'mean',
            'ctr': 'mean',
        }).reset_index()
        campaign_stats.columns = ['campaign_id', 'avg_spend', 'avg_ctr']
        
        # Merge back
        df = df.merge(campaign_stats, on='campaign_id', how='left')
        
        # Flag conditions
        df['spend_anomaly'] = df['spend'] > (df['avg_spend'] * 3)
        df['ctr_anomaly'] = (df['ctr'] == 0) & (df['impressions'] > 0)
        
        df['anomaly_flag'] = df['spend_anomaly'] | df['ctr_anomaly']
        
        # Clean up temp columns
        df = df.drop(columns=['avg_spend', 'avg_ctr', 'spend_anomaly', 'ctr_anomaly'])
        
        return df
