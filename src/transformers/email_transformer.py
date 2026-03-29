"""
Email Transformer
Transforms email platform data.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any

from .base_transformer import BaseTransformer


class EmailTransformer(BaseTransformer):
    """Transformer for email platform data."""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("email_transformer", config)
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform email data.
        
        Args:
            data: Input email DataFrame
            
        Returns:
            Transformed DataFrame
        """
        rows_in = len(data)
        self.logger.info(f"Transforming {rows_in} email records")
        
        df = data.copy()
        transformations = []
        
        # Add date column from sent_at
        if 'sent_at' in df.columns:
            df['date'] = pd.to_datetime(df['sent_at']).dt.strftime('%Y-%m-%d')
            transformations.append("extracted_date_from_sent_at")
        
        # Standardize email to match CRM
        if 'email' in df.columns:
            df['email'] = df['email'].str.lower().str.strip()
            transformations.append("standardized_email_format")
        
        # Calculate campaign-level metrics
        campaign_metrics = df.groupby('campaign_id').agg({
            'delivered': 'sum',
            'opened': 'sum',
            'clicked': 'sum',
            'bounced': 'sum',
            'unsubscribed': 'sum',
            'converted': 'sum',
        }).reset_index()
        
        total_sent = df.groupby('campaign_id').size().reset_index(name='total_sent')
        campaign_metrics = campaign_metrics.merge(total_sent, on='campaign_id')
        
        # Calculate rates
        campaign_metrics['delivery_rate'] = campaign_metrics['delivered'] / campaign_metrics['total_sent']
        campaign_metrics['open_rate'] = np.where(
            campaign_metrics['delivered'] > 0,
            campaign_metrics['opened'] / campaign_metrics['delivered'],
            0
        )
        campaign_metrics['click_rate'] = np.where(
            campaign_metrics['delivered'] > 0,
            campaign_metrics['clicked'] / campaign_metrics['delivered'],
            0
        )
        campaign_metrics['click_to_open_rate'] = np.where(
            campaign_metrics['opened'] > 0,
            campaign_metrics['clicked'] / campaign_metrics['opened'],
            0
        )
        campaign_metrics['bounce_rate'] = campaign_metrics['bounced'] / campaign_metrics['total_sent']
        campaign_metrics['unsubscribe_rate'] = campaign_metrics['unsubscribed'] / campaign_metrics['total_sent']
        
        # Flag deliverability issues
        campaign_metrics['deliverability_flag'] = (
            (campaign_metrics['bounce_rate'] > 0.05) |
            (campaign_metrics['delivery_rate'] < 0.90)
        )
        
        # Merge back to original data
        df = df.merge(
            campaign_metrics[['campaign_id', 'delivery_rate', 'open_rate', 'click_rate', 
                            'click_to_open_rate', 'bounce_rate', 'unsubscribe_rate', 'deliverability_flag']],
            on='campaign_id',
            how='left'
        )
        
        transformations.append("calculated_campaign_metrics")
        
        # Calculate engagement recency per contact
        if 'open_timestamp' in df.columns:
            df['open_timestamp'] = pd.to_datetime(df['open_timestamp'], errors='coerce')
            contact_last_open = df.groupby('email')['open_timestamp'].max().reset_index()
            contact_last_open.columns = ['email', 'last_open_date']
            contact_last_open['days_since_last_open'] = (
                pd.Timestamp.now() - contact_last_open['last_open_date']
            ).dt.days
            
            df = df.merge(contact_last_open[['email', 'days_since_last_open']], on='email', how='left')
            transformations.append("calculated_engagement_recency")
        
        # Update metadata
        self._update_metadata(
            rows_in=rows_in,
            rows_out=len(df),
            columns_added=['delivery_rate', 'open_rate', 'click_rate', 'click_to_open_rate',
                          'bounce_rate', 'unsubscribe_rate', 'deliverability_flag', 'days_since_last_open'],
            columns_removed=[],
            transformations=transformations,
        )
        
        self.logger.info(f"Email transformation complete: {len(df)} rows output")
        return df
    
    def calculate_contact_engagement(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate lifetime engagement metrics per contact."""
        contact_metrics = data.groupby('email').agg({
            'delivered': 'sum',
            'opened': 'sum',
            'clicked': 'sum',
            'converted': 'sum',
        }).reset_index()
        
        total_emails = data.groupby('email').size().reset_index(name='total_emails')
        contact_metrics = contact_metrics.merge(total_emails, on='email')
        
        contact_metrics['lifetime_open_rate'] = contact_metrics['opened'] / contact_metrics['delivered']
        contact_metrics['lifetime_click_rate'] = contact_metrics['clicked'] / contact_metrics['delivered']
        
        return contact_metrics
