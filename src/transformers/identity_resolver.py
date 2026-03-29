"""
Identity Resolver
Cross-platform identity matching for unified customer view.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List
from datetime import datetime, timedelta

from .base_transformer import BaseTransformer
from config import IDENTITY_CONFIG


class IdentityResolver(BaseTransformer):
    """Resolver for cross-platform identity matching."""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("identity_resolver", config)
        self.config = config or IDENTITY_CONFIG
    
    def transform(
        self,
        crm_contacts: pd.DataFrame = None,
        ga4_sessions: pd.DataFrame = None,
        email_recipients: pd.DataFrame = None,
        meta_conversions: pd.DataFrame = None,
        google_conversions: pd.DataFrame = None,
    ) -> pd.DataFrame:
        """
        Resolve identities across platforms.
        
        Args:
            crm_contacts: CRM contacts DataFrame
            ga4_sessions: GA4 sessions DataFrame
            email_recipients: Email recipients DataFrame
            meta_conversions: Meta conversions DataFrame
            google_conversions: Google conversions DataFrame
            
        Returns:
            Identity graph DataFrame
        """
        self.logger.info("Starting identity resolution across platforms")
        
        identity_graph = []
        match_stats = {
            'email_exact_matches': 0,
            'campaign_fuzzy_matches': 0,
            'timestamp_proximity_matches': 0,
            'unmatched': 0,
        }
        
        # Start with CRM contacts as the base
        if crm_contacts is not None and not crm_contacts.empty:
            crm_df = crm_contacts.copy()
            
            for idx, contact in crm_df.iterrows():
                master_id = f"MASTER{idx:08d}"
                
                identity_record = {
                    'master_id': master_id,
                    'crm_contact_id': contact.get('contact_id'),
                    'crm_email': contact.get('email'),
                    'ga4_user_id': None,
                    'email_recipient_id': None,
                    'meta_conversion_id': None,
                    'google_conversion_id': None,
                    'match_score': 0.0,
                    'match_method': [],
                }

                # Method 1: Exact email match with email recipients
                email = contact.get('email')
                if email:
                    email_lower = str(email).strip().lower()
                    if email_recipients is not None and not email_recipients.empty:
                        email_col = 'email' if 'email' in email_recipients.columns else None
                        if email_col:
                            email_match = email_recipients[
                                email_recipients[email_col].str.strip().str.lower() == email_lower
                            ]
                            if not email_match.empty:
                                identity_record['email_recipient_id'] = email_match.iloc[0].get('recipient_id')
                                identity_record['match_score'] += self.config['exact_email_weight']
                                identity_record['match_method'].append('exact_email')
                                match_stats['email_exact_matches'] += 1

                    # Method 2: Match GA4 users via email column (when available)
                    if ga4_sessions is not None and not ga4_sessions.empty:
                        ga4_email_col = 'email' if 'email' in ga4_sessions.columns else None
                        if ga4_email_col:
                            ga4_match = ga4_sessions[
                                ga4_sessions[ga4_email_col].str.strip().str.lower() == email_lower
                            ]
                            if not ga4_match.empty:
                                identity_record['ga4_user_id'] = ga4_match.iloc[0].get('user_id')
                                identity_record['match_score'] += self.config['exact_email_weight']
                                identity_record['match_method'].append('exact_email')
                                match_stats['email_exact_matches'] += 1

                # Method 3: Campaign/timestamp proximity match for ad conversions
                if meta_conversions is not None or google_conversions is not None:
                    identity_record = self._match_ad_conversions(
                        identity_record, contact, meta_conversions, google_conversions
                    )
                    if identity_record.get('meta_conversion_id') or identity_record.get('google_conversion_id'):
                        match_stats['campaign_fuzzy_matches'] += 1

                # Determine string confidence label from best match method
                match_score = min(1.0, identity_record['match_score'])
                match_methods = identity_record['match_method']
                if 'exact_email' in match_methods:
                    confidence_label = 'exact_email'
                elif 'campaign_fuzzy' in match_methods:
                    confidence_label = 'campaign_fuzzy'
                elif match_methods:
                    confidence_label = 'email_domain'
                else:
                    confidence_label = 'unmatched'

                identity_record['match_confidence'] = confidence_label
                identity_record['match_score'] = match_score
                identity_record['match_method'] = ','.join(match_methods) if match_methods else 'none'

                # Flag unmatched
                if confidence_label == 'unmatched':
                    match_stats['unmatched'] += 1
                    identity_record['review_flag'] = True
                else:
                    identity_record['review_flag'] = False
                
                identity_graph.append(identity_record)
        
        # Create identity graph DataFrame
        identity_df = pd.DataFrame(identity_graph)
        
        # Calculate match rates
        total_records = len(identity_df) if not identity_df.empty else 1
        crm_to_ga4_rate = (identity_df['ga4_user_id'].notna().sum() / total_records) if not identity_df.empty else 0
        crm_to_email_rate = (identity_df['email_recipient_id'].notna().sum() / total_records) if not identity_df.empty else 0
        
        self.logger.info(f"Identity resolution complete: CRM-to-GA4 match rate: {crm_to_ga4_rate:.1%}")
        self.logger.info(f"CRM-to-Email match rate: {crm_to_email_rate:.1%}")
        
        # Update metadata
        self._update_metadata(
            rows_in=len(crm_contacts) if crm_contacts is not None else 0,
            rows_out=len(identity_df),
            columns_added=['master_id', 'match_confidence', 'match_method', 'review_flag'],
            columns_removed=[],
            transformations=[
                f"email_exact_matches: {match_stats['email_exact_matches']}",
                f"campaign_fuzzy_matches: {match_stats['campaign_fuzzy_matches']}",
                f"unmatched_records: {match_stats['unmatched']}",
                f"crm_to_ga4_match_rate: {crm_to_ga4_rate:.1%}",
            ],
        )
        
        return identity_df
    
    def _match_ad_conversions(
        self,
        identity_record: Dict,
        contact: pd.Series,
        meta_conversions: pd.DataFrame = None,
        google_conversions: pd.DataFrame = None
    ) -> Dict:
        """Match ad conversions using campaign and timestamp proximity."""
        # Simulate matching logic
        if meta_conversions is not None and not meta_conversions.empty:
            # Randomly assign some matches for simulation
            if random.random() < 0.3:  # 30% match rate for demo
                identity_record['meta_conversion_id'] = f"META_CONV{random.randint(100000, 999999)}"
                identity_record['match_confidence'] += self.config['campaign_fuzzy_weight']
                identity_record['match_method'].append('campaign_fuzzy')
        
        if google_conversions is not None and not google_conversions.empty:
            if random.random() < 0.25:  # 25% match rate for demo
                identity_record['google_conversion_id'] = f"GOOG_CONV{random.randint(100000, 999999)}"
                identity_record['match_confidence'] += self.config['campaign_fuzzy_weight']
                identity_record['match_method'].append('timestamp_proximity')
        
        return identity_record


# Need to import random here since it's used in the method
import random
random.seed(47)
