"""
GA4 Extractor
Simulates extracting data from Google Analytics 4.
"""

import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

from .base_extractor import BaseExtractor


class GA4Extractor(BaseExtractor):
    """Extractor for Google Analytics 4 data."""
    
    def __init__(self, config: dict = None):
        default_config = {
            "name": "google_analytics_4",
            "schema_version": "1.0",
            "sessions_count": 5000,
            "events_count": 20000,
            "date_range_months": 18,
            "failure_rate": 0.05,
        }
        if config:
            default_config.update(config)
        super().__init__("ga4", default_config)
        self.fake = Faker()
        Faker.seed(45)
        random.seed(45)
        np.random.seed(45)
    
    def _generate_utm_campaigns(self) -> list:
        """Generate UTM campaigns that match ad platforms."""
        campaigns = []
        for i in range(100):
            source = np.random.choice(["google", "facebook", "instagram", "linkedin", "bing", "email", "direct"])
            medium = np.random.choice(["cpc", "ppc", "social", "email", "organic", "referral", "none"])
            campaign = f"Campaign_{random.randint(1, 80)}_{np.random.choice(['Spring', 'Summer', 'Fall', 'Winter'])}202{random.randint(3, 5)}"
            campaigns.append({
                "source": source,
                "medium": medium,
                "campaign": campaign,
                "full_utm": f"utm_source={source}&utm_medium={medium}&utm_campaign={campaign}"
            })
        return campaigns
    
    def _generate_sessions(self, count: int, utm_campaigns: list) -> pd.DataFrame:
        """Generate session-level data."""
        devices = ["desktop", "mobile", "tablet"]
        pages = ["/", "/pricing", "/features", "/about", "/contact", "/demo", "/trial", "/docs", "/blog", "/case-studies"]
        sources = ["google", "facebook", "instagram", "linkedin", "bing", "direct", "email", "organic"]
        
        sessions = []
        for i in range(count):
            session_date = self.fake.date_time_between(start_date="-18m", end_date="now")
            
            # Pick traffic source
            if random.random() < 0.4:  # 40% paid
                utm = np.random.choice(utm_campaigns)
                source = utm['source']
                medium = utm['medium']
                campaign = utm['campaign']
            else:
                source = np.random.choice(["organic", "direct", "referral"])
                medium = {"organic": "organic", "direct": "none", "referral": "referral"}[source]
                campaign = "(not set)"
            
            # Engagement metrics
            pages_per_session = max(1, int(np.random.exponential(3)) + 1)
            if pages_per_session > 50:  # Cap for realism
                pages_per_session = 50
            
            session_duration = int(np.random.exponential(120))
            if session_duration > 1800:
                session_duration = 1800
            
            bounce = pages_per_session == 1 and random.random() < 0.7
            engaged = session_duration > 60 or pages_per_session > 2
            
            # Conversion events
            conversion_types = ["demo_request", "trial_signup", "contact_form", "pricing_page_view", None]
            conversion = np.random.choice(conversion_types, p=[0.02, 0.015, 0.01, 0.05, 0.905])
            converted = conversion is not None
            
            sessions.append({
                "session_id": f"SESSION{random.randint(1000000000, 9999999999)}",
                "user_id": f"USER{random.randint(1000000, 9999999)}",
                "date": session_date.strftime("%Y-%m-%d"),
                "landing_page": np.random.choice(pages),
                "source": source,
                "medium": medium,
                "campaign": campaign,
                "device_category": np.random.choice(devices, p=[0.5, 0.4, 0.1]),
                "country": self.fake.country(),
                "city": self.fake.city(),
                "session_duration_seconds": session_duration,
                "pages_per_session": pages_per_session,
                "bounce": bounce,
                "engaged_session": engaged,
                "converted": converted,
                "conversion_type": conversion,
            })
        
        return pd.DataFrame(sessions)
    
    def _generate_events(self, sessions_df: pd.DataFrame) -> pd.DataFrame:
        """Generate event-level data from sessions."""
        event_names = ["page_view", "scroll", "click", "form_start", "form_submit", 
                      "file_download", "video_start", "video_complete", "purchase"]
        pages = ["/", "/pricing", "/features", "/about", "/contact", "/demo", "/trial", "/docs", "/blog"]
        
        events = []
        
        for _, session in sessions_df.iterrows():
            # Each session has multiple events
            num_events = max(1, session['pages_per_session'] + random.randint(0, 5))
            
            session_start = datetime.strptime(session['date'], "%Y-%m-%d")
            session_start = session_start.replace(hour=random.randint(0, 23), minute=random.randint(0, 59))
            
            for j in range(num_events):
                event_time = session_start + timedelta(seconds=j * random.randint(10, 120))
                
                event_name = np.random.choice(event_names, p=[0.4, 0.2, 0.15, 0.05, 0.05, 0.05, 0.05, 0.03, 0.02])
                
                events.append({
                    "event_id": f"EVT{random.randint(1000000000, 9999999999)}",
                    "session_id": session['session_id'],
                    "event_name": event_name,
                    "event_timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "page_path": np.random.choice(pages),
                    "event_value": random.randint(1, 100) if event_name in ["purchase", "form_submit"] else None,
                })
        
        return pd.DataFrame(events)
    
    def extract(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Extract GA4 data."""
        utm_campaigns = self._generate_utm_campaigns()
        
        self.logger.info(f"Generating {self.config['sessions_count']} GA4 sessions")
        sessions_df = self._generate_sessions(self.config['sessions_count'], utm_campaigns)
        
        # Limit events to configured count
        self.logger.info(f"Generating events from sessions")
        events_df = self._generate_events(sessions_df)
        
        # Sample events if we generated too many
        if len(events_df) > self.config['events_count']:
            events_df = events_df.sample(n=self.config['events_count'], random_state=42)
        
        # Store for later access
        self.sessions_df = sessions_df
        self.events_df = events_df
        
        # Return combined with type indicator
        combined = pd.concat([
            sessions_df.assign(record_type="session"),
            events_df.assign(record_type="event"),
        ], ignore_index=True)
        
        self.logger.info(f"Generated {len(sessions_df)} sessions, {len(events_df)} events")
        return combined
    
    def validate_response(self, data: pd.DataFrame) -> bool:
        """Validate GA4 data."""
        if data.empty:
            self.logger.error("No data extracted")
            return False
        
        required_cols = ["record_type"]
        if not all(col in data.columns for col in required_cols):
            self.logger.error(f"Missing required columns: {required_cols}")
            return False
        
        return True
