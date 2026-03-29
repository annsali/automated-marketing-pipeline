"""
CRM Transformer
Transforms and cleans CRM data.
"""

import re
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any

from .base_transformer import BaseTransformer


class CRMTransformer(BaseTransformer):
    """Transformer for CRM data."""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("crm_transformer", config)
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform CRM data.
        
        Args:
            data: Input CRM DataFrame
            
        Returns:
            Transformed DataFrame
        """
        rows_in = len(data)
        self.logger.info(f"Transforming {rows_in} CRM records")
        
        df = data.copy()
        transformations = []
        
        # Filter by record type if present
        if 'record_type' in df.columns:
            # Process each type separately or just pass through
            transformations.append("filtered_by_record_type")
        
        # Standardize lead status values
        if 'lead_status' in df.columns:
            status_map = {
                'marketing qualified': 'MQL',
                'marketing-qualified': 'MQL',
                'mql': 'MQL',
                'sales qualified': 'SQL',
                'sales-qualified': 'SQL',
                'sql': 'SQL',
                'lead': 'Lead',
                'opportunity': 'Opportunity',
                'customer': 'Customer',
                'churned': 'Churned',
            }
            df['lead_status'] = df['lead_status'].str.lower().map(status_map).fillna(df['lead_status'])
            transformations.append("standardized_lead_status")
        
        # Clean company names
        if 'name' in df.columns:
            df['name_clean'] = df['name'].apply(self._clean_company_name)
            df['name_original'] = df['name']
            df['name'] = df['name_clean']
            transformations.append("cleaned_company_names")
        
        # Validate email format
        if 'email' in df.columns:
            df['email_valid'] = df['email'].apply(self._validate_email)
            invalid_count = (~df['email_valid']).sum()
            transformations.append(f"validated_emails: {invalid_count} invalid")
        
        # Calculate derived fields
        if 'created_date' in df.columns:
            df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
            df['account_age_days'] = (datetime.now() - df['created_date']).dt.days
            transformations.append("calculated_account_age_days")
        
        if 'last_activity_date' in df.columns:
            df['last_activity_date'] = pd.to_datetime(df['last_activity_date'], errors='coerce')
            df['days_since_last_activity'] = (datetime.now() - df['last_activity_date']).dt.days
            transformations.append("calculated_days_since_last_activity")
        
        if 'mql_date' in df.columns and 'created_date' in df.columns:
            df['mql_date'] = pd.to_datetime(df['mql_date'], errors='coerce')
            df['lead_age_days'] = (df['mql_date'] - df['created_date']).dt.days
            transformations.append("calculated_lead_age_days")
        
        # Calculate derived metrics for opportunities
        if 'amount' in df.columns and 'close_date' in df.columns and 'created_date' in df.columns:
            df['close_date'] = pd.to_datetime(df['close_date'], errors='coerce')
            df['sales_cycle_days'] = (df['close_date'] - df['created_date']).dt.days
            transformations.append("calculated_sales_cycle_days")
        
        # Update metadata
        self._update_metadata(
            rows_in=rows_in,
            rows_out=len(df),
            columns_added=['name_clean', 'name_original', 'email_valid', 'account_age_days', 
                          'days_since_last_activity', 'lead_age_days', 'sales_cycle_days'] if 'amount' in df.columns else [],
            columns_removed=[],
            transformations=transformations,
        )
        
        self.logger.info(f"CRM transformation complete: {len(df)} rows output")
        return df
    
    def _clean_company_name(self, name: str) -> str:
        """Remove legal suffixes from company name for matching."""
        if not isinstance(name, str):
            return name
        
        suffixes = [
            r'\s+Inc\.?$', r'\s+LLC\.?$', r'\s+L\.?L\.?C\.?$',
            r'\s+Corp\.?$', r'\s+Corporation\.?$', r'\s+Ltd\.?$',
            r'\s+Limited\.?$', r'\s+LP\.?$', r'\s+L\.?P\.?$',
            r'\s+PLC\.?$', r'\s+GmbH\.?$', r'\s+S\.?A\.?$',
        ]
        
        name = name.strip()
        for suffix in suffixes:
            name = re.sub(suffix, '', name, flags=re.IGNORECASE)
        
        return name.strip()
    
    def _validate_email(self, email: str) -> bool:
        """Validate email format using regex."""
        if not isinstance(email, str):
            return False
        
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
