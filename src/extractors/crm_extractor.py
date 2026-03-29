"""
CRM Extractor (Salesforce)
Simulates extracting data from Salesforce CRM.
"""

import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

from .base_extractor import BaseExtractor


class CRMExtractor(BaseExtractor):
    """Extractor for Salesforce CRM data."""
    
    def __init__(self, config: dict = None):
        default_config = {
            "name": "salesforce_crm",
            "schema_version": "1.0",
            "accounts_count": 500,
            "contacts_count": 1500,
            "opportunities_count": 300,
            "activities_count": 2000,
            "failure_rate": 0.05,
        }
        if config:
            default_config.update(config)
        super().__init__("crm", default_config)
        self.fake = Faker()
        Faker.seed(42)
        random.seed(42)
        np.random.seed(42)
    
    def _generate_salesforce_id(self, prefix: str) -> str:
        """Generate a realistic Salesforce ID."""
        suffix = ''.join(random.choices('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=15))
        return f"{prefix}{suffix}"
    
    def _generate_accounts(self, count: int) -> pd.DataFrame:
        """Generate account records."""
        industries = ["Technology", "Healthcare", "Finance", "Retail", "Manufacturing", 
                      "Energy", "Education", "Government", "Media", "Consulting"]
        regions = ["North America", "EMEA", "APAC", "Latin America"]
        
        accounts = []
        for i in range(count):
            created_date = self.fake.date_time_between(start_date="-5y", end_date="now")
            accounts.append({
                "account_id": self._generate_salesforce_id("001"),
                "name": self.fake.company(),
                "industry": np.random.choice(industries),
                "employee_count": np.random.choice([10, 50, 100, 500, 1000, 5000, 10000]),
                "annual_revenue": np.random.choice([100000, 500000, 1000000, 5000000, 10000000, 50000000]),
                "region": np.random.choice(regions),
                "owner": self.fake.name(),
                "created_date": created_date.strftime("%Y-%m-%d"),
                "salesforce_id": self._generate_salesforce_id("001"),
                "account_status": np.random.choice(["Active", "Inactive", "Prospect"], p=[0.7, 0.1, 0.2]),
            })
        
        return pd.DataFrame(accounts)
    
    def _generate_contacts(self, count: int, account_ids: list) -> pd.DataFrame:
        """Generate contact records."""
        lead_statuses = ["Lead", "MQL", "SQL", "Opportunity", "Customer", "Churned"]
        lead_sources = ["Website", "Trade Show", "Referral", "Paid Social", "Paid Search", 
                       "Organic", "Email", "Sales Outreach", "Partner"]
        departments = ["Sales", "Marketing", "Engineering", "Finance", "Operations", "HR", "Executive"]
        
        contacts = []
        for i in range(count):
            created_date = self.fake.date_time_between(start_date="-3y", end_date="now")
            last_activity = self.fake.date_time_between(start_date=created_date, end_date="now")
            
            # Generate MQL/SQL dates based on lead status
            mql_date = None
            sql_date = None
            status = np.random.choice(lead_statuses, p=[0.2, 0.25, 0.2, 0.15, 0.15, 0.05])
            
            if status in ["MQL", "SQL", "Opportunity", "Customer"]:
                mql_date = (created_date + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
            if status in ["SQL", "Opportunity", "Customer"]:
                sql_date = (created_date + timedelta(days=random.randint(15, 60))).strftime("%Y-%m-%d")
            
            contacts.append({
                "contact_id": self._generate_salesforce_id("003"),
                "account_id": np.random.choice(account_ids),
                "email": self.fake.email().lower(),
                "name": self.fake.name(),
                "title": self.fake.job(),
                "department": np.random.choice(departments),
                "lead_status": status,
                "lead_source": np.random.choice(lead_sources),
                "created_date": created_date.strftime("%Y-%m-%d"),
                "last_activity_date": last_activity.strftime("%Y-%m-%d"),
                "mql_date": mql_date,
                "sql_date": sql_date,
                "email_opt_in": np.random.choice([True, False], p=[0.85, 0.15]),
            })
        
        return pd.DataFrame(contacts)
    
    def _generate_opportunities(self, count: int, account_ids: list, contact_ids: list) -> pd.DataFrame:
        """Generate opportunity records."""
        stages = ["Prospecting", "Qualification", "Needs Analysis", "Value Proposition", 
                  "Id. Decision Makers", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
        product_lines = ["Enterprise", "Professional", "Starter", "Add-ons", "Services"]
        
        opportunities = []
        for i in range(count):
            created_date = self.fake.date_time_between(start_date="-2y", end_date="now")
            close_date = created_date + timedelta(days=random.randint(30, 180))
            stage = np.random.choice(stages, p=[0.1, 0.1, 0.1, 0.1, 0.1, 0.15, 0.15, 0.1, 0.1])
            is_won = stage == "Closed Won"
            
            # Win probability based on stage
            win_prob_map = {
                "Prospecting": 0.1, "Qualification": 0.2, "Needs Analysis": 0.3,
                "Value Proposition": 0.4, "Id. Decision Makers": 0.5, "Proposal": 0.6,
                "Negotiation": 0.75, "Closed Won": 1.0, "Closed Lost": 0.0
            }
            
            opportunities.append({
                "opp_id": self._generate_salesforce_id("006"),
                "account_id": random.choice(account_ids),
                "contact_id": random.choice(contact_ids),
                "stage": stage,
                "amount": random.choice([5000, 15000, 30000, 50000, 100000, 250000, 500000]),
                "created_date": created_date.strftime("%Y-%m-%d"),
                "close_date": close_date.strftime("%Y-%m-%d"),
                "product_line": np.random.choice(product_lines),
                "win_probability": win_prob_map[stage],
                "is_won": is_won,
            })
        
        return pd.DataFrame(opportunities)
    
    def _generate_activities(self, count: int, contact_ids: list) -> pd.DataFrame:
        """Generate activity records."""
        activity_types = ["Call", "Meeting", "Email", "Task"]
        
        activities = []
        for i in range(count):
            activity_date = self.fake.date_time_between(start_date="-2y", end_date="now")
            activity_type = np.random.choice(activity_types, p=[0.3, 0.2, 0.35, 0.15])
            
            duration = 0
            if activity_type == "Call":
                duration = random.randint(5, 60)
            elif activity_type == "Meeting":
                duration = random.randint(15, 120)
            
            activities.append({
                "activity_id": f"ACT{random.randint(100000, 999999)}",
                "contact_id": random.choice(contact_ids),
                "type": activity_type,
                "subject": self.fake.sentence(nb_words=4),
                "date": activity_date.strftime("%Y-%m-%d"),
                "duration_minutes": duration,
                "assigned_to": self.fake.name(),
            })
        
        return pd.DataFrame(activities)
    
    def extract(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Extract CRM data."""
        self.logger.info(f"Generating CRM data: {self.config['accounts_count']} accounts, "
                        f"{self.config['contacts_count']} contacts, "
                        f"{self.config['opportunities_count']} opportunities, "
                        f"{self.config['activities_count']} activities")
        
        # Generate accounts first (needed for contacts and opportunities)
        accounts_df = self._generate_accounts(self.config['accounts_count'])
        account_ids = accounts_df['account_id'].tolist()
        
        # Generate contacts
        contacts_df = self._generate_contacts(self.config['contacts_count'], account_ids)
        contact_ids = contacts_df['contact_id'].tolist()
        
        # Generate opportunities
        opportunities_df = self._generate_opportunities(
            self.config['opportunities_count'], account_ids, contact_ids
        )
        
        # Generate activities
        activities_df = self._generate_activities(self.config['activities_count'], contact_ids)
        
        # Store all dataframes for later access
        self.accounts_df = accounts_df
        self.contacts_df = contacts_df
        self.opportunities_df = opportunities_df
        self.activities_df = activities_df
        
        # Return combined metadata
        combined = pd.concat([
            accounts_df.assign(record_type="account"),
            contacts_df.assign(record_type="contact"),
            opportunities_df.assign(record_type="opportunity"),
            activities_df.assign(record_type="activity"),
        ], ignore_index=True)
        
        return combined
    
    def validate_response(self, data: pd.DataFrame) -> bool:
        """Validate CRM data."""
        if data.empty:
            self.logger.error("No data extracted")
            return False
        
        required_cols = ["record_type"]
        if not all(col in data.columns for col in required_cols):
            self.logger.error(f"Missing required columns: {required_cols}")
            return False
        
        return True
