"""
Email Platform Extractor (SFMC)
Simulates extracting data from Salesforce Marketing Cloud.
"""

import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

from .base_extractor import BaseExtractor


class EmailPlatformExtractor(BaseExtractor):
    """Extractor for Email Platform (SFMC) data."""
    
    def __init__(self, config: dict = None):
        default_config = {
            "name": "sfmc_email",
            "schema_version": "1.0",
            "campaigns": 10,
            "sends_count": 5000,
            "date_range_months": 18,
            "failure_rate": 0.05,
        }
        if config:
            default_config.update(config)
        super().__init__("email", default_config)
        self.fake = Faker()
        Faker.seed(46)
        random.seed(46)
        np.random.seed(46)
    
    def _generate_campaigns(self, count: int) -> list:
        """Generate email campaign definitions."""
        campaign_types = ["Newsletter", "Promotional", "Welcome", "Re-engagement", 
                         "Product Launch", "Event", "Nurture"]
        
        campaigns = []
        for i in range(count):
            c_type = np.random.choice(campaign_types, p=[0.3, 0.25, 0.1, 0.1, 0.1, 0.08, 0.07])
            send_date = self.fake.date_time_between(start_date="-18m", end_date="now")
            
            campaigns.append({
                "campaign_id": f"EMAIL{random.randint(10000, 99999)}",
                "campaign_name": f"{c_type}_{self.fake.word().title()}_{send_date.strftime('%Y%m')}",
                "campaign_type": c_type,
                "send_date": send_date,
                "subject_line": self.fake.sentence(nb_words=6),
                "list_size": random.choice([5000, 10000, 25000, 50000, 100000]),
                "segment_name": f"Segment_{random.choice(['Active', 'Engaged', 'Lapsed', 'New', 'VIP'])}",
            })
        return campaigns
    
    def _generate_recipients(self, campaigns: list, total_sends: int) -> pd.DataFrame:
        """Generate recipient-level email data."""
        recipients = []
        
        # Distribute sends across campaigns
        sends_per_campaign = total_sends // len(campaigns)
        
        for campaign in campaigns:
            campaign_sends = min(sends_per_campaign, campaign['list_size'])
            
            # Adjust rates based on campaign type
            c_type = campaign['campaign_type']
            if c_type == "Welcome":
                open_rate, click_rate, bounce_rate, unsub_rate = 0.35, 0.08, 0.015, 0.005
            elif c_type == "Re-engagement":
                open_rate, click_rate, bounce_rate, unsub_rate = 0.12, 0.015, 0.04, 0.008
            else:  # Regular campaigns
                open_rate, click_rate, bounce_rate, unsub_rate = 0.21, 0.032, 0.02, 0.003
            
            for i in range(campaign_sends):
                email = self.fake.email().lower()
                
                # Simulate delivery (97% average)
                delivered = random.random() < 0.97
                
                if not delivered:
                    bounced = True
                    bounce_type = random.choice(["hard", "soft"])
                else:
                    bounced = False
                    bounce_type = None
                
                # Open (only if delivered and not bounced)
                opened = delivered and not bounced and random.random() < open_rate
                open_timestamp = None
                if opened:
                    open_delay = timedelta(minutes=random.randint(1, 1440))
                    open_timestamp = (campaign['send_date'] + open_delay).strftime("%Y-%m-%d %H:%M:%S")
                
                # Click (only if opened)
                clicked = opened and random.random() < (click_rate / open_rate)
                click_timestamp = None
                if clicked:
                    click_delay = timedelta(minutes=random.randint(1, 60))
                    if open_timestamp:
                        open_dt = datetime.strptime(open_timestamp, "%Y-%m-%d %H:%M:%S")
                        click_timestamp = (open_dt + click_delay).strftime("%Y-%m-%d %H:%M:%S")
                
                # Unsubscribe (if delivered)
                unsubscribed = delivered and not bounced and random.random() < unsub_rate
                
                # Conversion (if clicked)
                converted = clicked and random.random() < 0.15
                conversion_timestamp = None
                if converted and click_timestamp:
                    click_dt = datetime.strptime(click_timestamp, "%Y-%m-%d %H:%M:%S")
                    conv_delay = timedelta(hours=random.randint(1, 72))
                    conversion_timestamp = (click_dt + conv_delay).strftime("%Y-%m-%d %H:%M:%S")
                
                recipients.append({
                    "recipient_id": f"REC{random.randint(10000000, 99999999)}",
                    "email": email,
                    "campaign_id": campaign['campaign_id'],
                    "sent_at": campaign['send_date'].strftime("%Y-%m-%d %H:%M:%S"),
                    "delivered": delivered,
                    "opened": opened,
                    "open_timestamp": open_timestamp,
                    "clicked": clicked,
                    "click_timestamp": click_timestamp,
                    "bounced": bounced,
                    "bounce_type": bounce_type,
                    "unsubscribed": unsubscribed,
                    "converted": converted,
                    "conversion_timestamp": conversion_timestamp,
                })
        
        return pd.DataFrame(recipients)
    
    def extract(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Extract email platform data."""
        campaigns = self._generate_campaigns(self.config['campaigns'])
        
        # Create campaigns DataFrame
        campaigns_df = pd.DataFrame([{
            "campaign_id": c['campaign_id'],
            "campaign_name": c['campaign_name'],
            "send_date": c['send_date'].strftime("%Y-%m-%d"),
            "subject_line": c['subject_line'],
            "list_size": c['list_size'],
            "segment_name": c['segment_name'],
        } for c in campaigns])
        
        self.campaigns_df = campaigns_df
        
        # Generate recipients
        self.logger.info(f"Generating {self.config['sends_count']} email sends across {len(campaigns)} campaigns")
        recipients_df = self._generate_recipients(campaigns, self.config['sends_count'])
        self.recipients_df = recipients_df
        
        # Return recipients as main data (campaigns are metadata)
        return recipients_df
    
    def validate_response(self, data: pd.DataFrame) -> bool:
        """Validate email platform data."""
        if data.empty:
            self.logger.error("No data extracted")
            return False
        
        required_cols = ["recipient_id", "email", "campaign_id", "sent_at", "delivered"]
        if not all(col in data.columns for col in required_cols):
            self.logger.error(f"Missing required columns: {required_cols}")
            return False
        
        return True
