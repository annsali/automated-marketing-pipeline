"""
Google Ads Extractor
Simulates extracting data from Google Ads API.
"""

import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

from .base_extractor import BaseExtractor


class GoogleAdsExtractor(BaseExtractor):
    """Extractor for Google Ads data."""
    
    def __init__(self, config: dict = None):
        default_config = {
            "name": "google_ads",
            "schema_version": "1.0",
            "campaigns": 8,
            "date_range_months": 18,
            "failure_rate": 0.05,
        }
        if config:
            default_config.update(config)
        super().__init__("google_ads", default_config)
        self.fake = Faker()
        Faker.seed(44)
        random.seed(44)
        np.random.seed(44)
    
    def _generate_campaigns(self, count: int) -> list:
        """Generate campaign definitions."""
        campaign_types = ["Search", "Display", "Video", "Performance_Max"]
        
        campaigns = []
        for i in range(count):
            c_type = np.random.choice(campaign_types, p=[0.4, 0.25, 0.15, 0.2])
            campaigns.append({
                "campaign_id": f"{random.randint(1000000000, 9999999999)}",
                "campaign_name": f"Google_{c_type}_{self.fake.word().title()}_{random.randint(1,4)}Q202{random.randint(3,5)}",
                "campaign_type": c_type,
                "daily_budget": random.choice([50, 100, 200, 500]),
            })
        return campaigns
    
    def extract(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Extract Google Ads data."""
        campaigns = self._generate_campaigns(self.config['campaigns'])
        devices = ["desktop", "mobile", "tablet"]
        networks = ["Search", "Display", "YouTube"]
        
        records = []
        current_date = start_date
        
        while current_date <= end_date:
            for campaign in campaigns:
                # Skip some days
                if random.random() < 0.08:
                    continue
                
                # Weekday vs weekend
                is_weekend = current_date.weekday() >= 5
                day_multiplier = 0.75 if is_weekend else 1.0
                
                # Campaign type affects metrics
                c_type = campaign['campaign_type']
                if c_type == "Search":
                    cpc_range = (2.0, 8.0)
                    ctr_range = (0.03, 0.08)
                    conv_rate = 0.06
                elif c_type == "Display":
                    cpc_range = (0.3, 1.5)
                    ctr_range = (0.005, 0.02)
                    conv_rate = 0.02
                elif c_type == "Video":
                    cpc_range = (0.1, 0.8)
                    ctr_range = (0.01, 0.04)
                    conv_rate = 0.03
                else:  # Performance Max
                    cpc_range = (0.5, 3.0)
                    ctr_range = (0.01, 0.04)
                    conv_rate = 0.04
                
                base_spend = campaign['daily_budget'] * day_multiplier * random.uniform(0.7, 1.15)
                # Google reports in micros
                cost_micros = int(base_spend * 1_000_000)
                cost = cost_micros / 1_000_000
                
                cpc = random.uniform(*cpc_range)
                clicks = int(cost / cpc)
                ctr = random.uniform(*ctr_range)
                impressions = int(clicks / ctr) if ctr > 0 else 0
                
                conversions = int(clicks * random.uniform(conv_rate * 0.5, conv_rate * 1.5))
                conversion_value = conversions * random.uniform(40, 250)
                
                records.append({
                    "date": current_date.strftime("%Y-%m-%d"),
                    "campaign_id": campaign['campaign_id'],
                    "campaign_name": campaign['campaign_name'],
                    "ad_group_id": f"{random.randint(10000000000, 99999999999)}",
                    "ad_group_name": f"AdGroup_{self.fake.word().title()}",
                    "impressions": impressions,
                    "clicks": clicks,
                    "cost_micros": cost_micros,
                    "cost": round(cost, 2),
                    "conversions": conversions,
                    "conversion_value": round(conversion_value, 2),
                    "search_impression_share": round(random.uniform(0.45, 0.85), 2),
                    "avg_position_proxy": round(random.uniform(1.5, 4.5), 1),
                    "quality_score": random.randint(3, 10),
                    "campaign_type": campaign['campaign_type'],
                    "device": np.random.choice(devices, p=[0.45, 0.45, 0.1]),
                    "network": random.choice(networks),
                    "search_terms": self.fake.word(),
                    "keyword_match_type": random.choice(["EXACT", "PHRASE", "BROAD"]),
                })
            
            current_date += timedelta(days=1)
        
        self.logger.info(f"Generated {len(records)} Google Ads records across {len(campaigns)} campaigns")
        return pd.DataFrame(records)
    
    def validate_response(self, data: pd.DataFrame) -> bool:
        """Validate Google Ads data."""
        if data.empty:
            self.logger.error("No data extracted")
            return False
        
        required_cols = ["date", "campaign_id", "impressions", "clicks", "cost"]
        if not all(col in data.columns for col in required_cols):
            self.logger.error(f"Missing required columns: {required_cols}")
            return False
        
        if (data['cost'] < 0).any():
            self.logger.error("Negative cost values found")
            return False
        
        return True
