"""
Meta Ads Extractor
Simulates extracting data from Meta Ads API.
"""

import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

from .base_extractor import BaseExtractor


class MetaAdsExtractor(BaseExtractor):
    """Extractor for Meta Ads data."""
    
    def __init__(self, config: dict = None):
        default_config = {
            "name": "meta_ads",
            "schema_version": "1.0",
            "campaigns": 10,
            "date_range_months": 18,
            "failure_rate": 0.05,
        }
        if config:
            default_config.update(config)
        super().__init__("meta_ads", default_config)
        self.fake = Faker()
        Faker.seed(43)
        random.seed(43)
        np.random.seed(43)
    
    def _generate_campaigns(self, count: int) -> list:
        """Generate campaign definitions."""
        objectives = ["CONVERSIONS", "TRAFFIC", "LEAD_GENERATION", "AWARENESS"]
        platforms = ["Facebook", "Instagram", "Audience_Network", "Messenger"]
        import numpy as np
        
        campaigns = []
        for i in range(count):
            obj = np.random.choice(objectives)
            campaigns.append({
                "campaign_id": f"2384{random.randint(1000000000, 9999999999)}",
                "campaign_name": f"{obj}_{self.fake.word().title()}_{self.fake.word().title()}_Q{random.randint(1,4)}202{random.randint(3,5)}",
                "objective": obj,
                "platform": random.choice(platforms),
                "daily_budget": random.choice([50, 100, 200, 500, 1000]),
            })
        return campaigns
    
    def extract(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Extract Meta Ads data."""
        campaigns = self._generate_campaigns(self.config['campaigns'])
        date_range = (end_date - start_date).days
        
        records = []
        current_date = start_date
        
        while current_date <= end_date:
            for campaign in campaigns:
                # Skip some days (paused campaigns)
                if random.random() < 0.05:
                    continue
                
                # Weekday vs weekend patterns
                is_weekend = current_date.weekday() >= 5
                day_multiplier = 0.7 if is_weekend else 1.0
                
                # Seasonal spike in Q4
                if current_date.month in [10, 11, 12]:
                    day_multiplier *= 1.3
                
                # Increasing CPMs over time
                months_elapsed = (current_date - start_date).days / 30
                cpm_inflation = 1 + (months_elapsed * 0.02)
                
                base_spend = campaign['daily_budget'] * day_multiplier * random.uniform(0.8, 1.1)
                spend = base_spend * cpm_inflation
                
                # Derive metrics
                cpm = 8.0 * cpm_inflation * random.uniform(0.8, 1.2)
                impressions = int(spend / (cpm / 1000))
                ctr = random.uniform(0.005, 0.025) if campaign['objective'] != 'AWARENESS' else random.uniform(0.003, 0.01)
                clicks = int(impressions * ctr)
                cpc = spend / clicks if clicks > 0 else 0
                
                conversions = int(clicks * random.uniform(0.02, 0.08)) if clicks > 0 else 0
                conversion_value = conversions * random.uniform(50, 200)
                cost_per_conversion = spend / conversions if conversions > 0 else 0
                
                reach = int(impressions / random.uniform(1.2, 2.5))
                frequency = impressions / reach if reach > 0 else 0
                
                records.append({
                    "date": current_date.strftime("%Y-%m-%d"),
                    "campaign_id": campaign['campaign_id'],
                    "campaign_name": campaign['campaign_name'],
                    "ad_set_id": f"2384{random.randint(1000000000, 9999999999)}",
                    "ad_set_name": f"AdSet_{self.fake.word().title()}",
                    "ad_id": f"2384{random.randint(1000000000, 9999999999)}",
                    "impressions": impressions,
                    "clicks": clicks,
                    "spend": round(spend, 2),
                    "conversions": conversions,
                    "conversion_value": round(conversion_value, 2),
                    "reach": reach,
                    "frequency": round(frequency, 2),
                    "cpm": round(cpm, 2),
                    "cpc": round(cpc, 2),
                    "ctr": round(ctr, 4),
                    "cost_per_conversion": round(cost_per_conversion, 2),
                    "video_views": int(impressions * random.uniform(0.1, 0.4)),
                    "video_completions": int(impressions * random.uniform(0.02, 0.1)),
                    "landing_page_views": int(clicks * random.uniform(0.6, 0.9)),
                    "link_clicks": clicks,
                    "objective": campaign['objective'],
                    "platform": campaign['platform'],
                    "relevance_score": random.randint(1, 10),
                    "quality_ranking": random.choice(["BELOW_AVERAGE", "AVERAGE", "ABOVE_AVERAGE"]),
                    "engagement_rate_ranking": random.choice(["BELOW_AVERAGE", "AVERAGE", "ABOVE_AVERAGE"]),
                })
            
            current_date += timedelta(days=1)
        
        self.logger.info(f"Generated {len(records)} Meta Ads records across {len(campaigns)} campaigns")
        return pd.DataFrame(records)
    
    def validate_response(self, data: pd.DataFrame) -> bool:
        """Validate Meta Ads data."""
        if data.empty:
            self.logger.error("No data extracted")
            return False
        
        required_cols = ["date", "campaign_id", "impressions", "clicks", "spend"]
        if not all(col in data.columns for col in required_cols):
            self.logger.error(f"Missing required columns: {required_cols}")
            return False
        
        # Check for negative values
        if (data['spend'] < 0).any():
            self.logger.error("Negative spend values found")
            return False
        
        return True
