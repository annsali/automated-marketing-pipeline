"""
Warehouse Loader
Loads transformed data into the unified data warehouse.
"""

import logging
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from config import WAREHOUSE_DB


class WarehouseLoader:
    """Loader for the unified marketing data warehouse."""
    
    def __init__(self, db_path: Path = None):
        self.db_path = db_path or WAREHOUSE_DB
        self.logger = logging.getLogger("warehouse_loader")
        self.load_metadata: Dict[str, Any] = {}
        
        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def create_tables(self):
        """Create warehouse tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Dimension tables
        cursor.executescript("""
            -- Date dimension
            CREATE TABLE IF NOT EXISTS dim_dates (
                date TEXT PRIMARY KEY,
                day_of_week INTEGER,
                week_number INTEGER,
                month INTEGER,
                quarter INTEGER,
                year INTEGER,
                is_weekend INTEGER,
                is_holiday INTEGER,
                fiscal_quarter INTEGER
            );
            
            -- Accounts dimension
            CREATE TABLE IF NOT EXISTS dim_accounts (
                account_id TEXT PRIMARY KEY,
                name TEXT,
                name_original TEXT,
                industry TEXT,
                employee_count INTEGER,
                annual_revenue REAL,
                revenue_tier TEXT,
                region TEXT,
                icp_score INTEGER,
                created_date TEXT,
                owner TEXT,
                account_status TEXT
            );
            
            -- Contacts dimension
            CREATE TABLE IF NOT EXISTS dim_contacts (
                contact_id TEXT PRIMARY KEY,
                master_id TEXT,
                account_id TEXT,
                email TEXT,
                email_valid INTEGER,
                name TEXT,
                title TEXT,
                department TEXT,
                lead_status TEXT,
                lead_source TEXT,
                created_date TEXT,
                last_activity_date TEXT,
                mql_date TEXT,
                sql_date TEXT,
                email_opt_in INTEGER
            );
            
            -- Campaigns dimension
            CREATE TABLE IF NOT EXISTS dim_campaigns (
                campaign_id TEXT PRIMARY KEY,
                campaign_name TEXT,
                platform TEXT,
                channel TEXT,
                campaign_type TEXT,
                objective TEXT,
                start_date TEXT,
                end_date TEXT,
                budget REAL,
                target_segment TEXT
            );
            
            -- Fact tables
            CREATE TABLE IF NOT EXISTS fact_ad_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                platform TEXT,
                campaign_id TEXT,
                ad_group_id TEXT,
                ad_id TEXT,
                channel TEXT,
                impressions INTEGER,
                clicks INTEGER,
                spend REAL,
                conversions INTEGER,
                conversion_value REAL,
                ctr REAL,
                cpc REAL,
                cpa REAL,
                roas REAL,
                device TEXT,
                quality_score INTEGER,
                loaded_at TEXT
            );
            
            CREATE TABLE IF NOT EXISTS fact_web_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                session_id TEXT,
                user_id TEXT,
                master_id TEXT,
                channel TEXT,
                channel_group TEXT,
                landing_page TEXT,
                source TEXT,
                medium TEXT,
                campaign TEXT,
                device_category TEXT,
                country TEXT,
                city TEXT,
                session_duration_seconds INTEGER,
                pages_per_session INTEGER,
                bounce INTEGER,
                engaged_session INTEGER,
                converted INTEGER,
                conversion_type TEXT,
                engagement_score REAL,
                is_bot INTEGER,
                loaded_at TEXT
            );
            
            CREATE TABLE IF NOT EXISTS fact_email_engagement (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                campaign_id TEXT,
                recipient_id TEXT,
                master_id TEXT,
                email TEXT,
                delivered INTEGER,
                opened INTEGER,
                clicked INTEGER,
                bounced INTEGER,
                unsubscribed INTEGER,
                converted INTEGER,
                open_timestamp TEXT,
                click_timestamp TEXT,
                conversion_timestamp TEXT,
                loaded_at TEXT
            );
            
            CREATE TABLE IF NOT EXISTS fact_crm_activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                activity_id TEXT,
                contact_id TEXT,
                master_id TEXT,
                activity_type TEXT,
                subject TEXT,
                duration_minutes INTEGER,
                assigned_to TEXT,
                loaded_at TEXT
            );
            
            CREATE TABLE IF NOT EXISTS fact_pipeline (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                opp_id TEXT,
                account_id TEXT,
                master_id TEXT,
                stage TEXT,
                amount REAL,
                product_line TEXT,
                is_won INTEGER,
                win_probability REAL,
                created_date TEXT,
                close_date TEXT,
                sales_cycle_days INTEGER,
                loaded_at TEXT
            );
            
            -- Load tracking
            CREATE TABLE IF NOT EXISTS load_tracking (
                table_name TEXT PRIMARY KEY,
                last_loaded_date TEXT,
                row_count INTEGER,
                load_duration_seconds REAL,
                load_type TEXT
            );
            
            CREATE INDEX IF NOT EXISTS idx_ad_perf_date ON fact_ad_performance(date);
            CREATE INDEX IF NOT EXISTS idx_ad_perf_campaign ON fact_ad_performance(campaign_id);
            CREATE INDEX IF NOT EXISTS idx_web_sessions_date ON fact_web_sessions(date);
            CREATE INDEX IF NOT EXISTS idx_email_date ON fact_email_engagement(date);
            CREATE INDEX IF NOT EXISTS idx_pipeline_date ON fact_pipeline(date);
        """)
        
        conn.commit()
        conn.close()
        self.logger.info("Warehouse tables created/verified")
    
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'append'
    ) -> int:
        """
        Load a DataFrame into a warehouse table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: 'append', 'replace', or 'fail'
            
        Returns:
            Number of rows loaded
        """
        if df.empty:
            self.logger.warning(f"Empty DataFrame provided for {table_name}")
            return 0
        
        # Add loaded timestamp
        df = df.copy()
        df['loaded_at'] = datetime.utcnow().isoformat()
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            rows_before = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", conn).iloc[0]['cnt']
        except:
            rows_before = 0
        
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        
        rows_after = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", conn).iloc[0]['cnt']
        rows_loaded = rows_after - rows_before if if_exists == 'append' else rows_after
        
        conn.close()
        
        self.logger.info(f"Loaded {rows_loaded} rows into {table_name}")
        
        # Update load tracking
        self._update_load_tracking(table_name, rows_loaded)
        
        return rows_loaded
    
    def _update_load_tracking(self, table_name: str, row_count: int):
        """Update load tracking metadata."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO load_tracking (table_name, last_loaded_date, row_count, load_duration_seconds, load_type)
            VALUES (?, ?, ?, ?, ?)
        """, (table_name, datetime.utcnow().isoformat(), row_count, 0, 'full'))
        
        conn.commit()
        conn.close()
    
    def verify_referential_integrity(self) -> List[Dict]:
        """Verify foreign key relationships."""
        issues = []
        conn = sqlite3.connect(self.db_path)
        
        # Check contacts -> accounts
        orphans = pd.read_sql("""
            SELECT COUNT(*) as cnt FROM dim_contacts c
            LEFT JOIN dim_accounts a ON c.account_id = a.account_id
            WHERE a.account_id IS NULL
        """, conn)
        
        if orphans.iloc[0]['cnt'] > 0:
            issues.append({
                'relationship': 'contacts->accounts',
                'orphan_count': int(orphans.iloc[0]['cnt'])
            })
        
        conn.close()
        
        if issues:
            self.logger.warning(f"Referential integrity issues found: {issues}")
        else:
            self.logger.info("Referential integrity verified")
        
        return issues
    
    def get_table_stats(self) -> Dict[str, int]:
        """Get row counts for all warehouse tables."""
        conn = sqlite3.connect(self.db_path)
        
        tables = [
            'dim_dates', 'dim_accounts', 'dim_contacts', 'dim_campaigns',
            'fact_ad_performance', 'fact_web_sessions', 'fact_email_engagement',
            'fact_crm_activities', 'fact_pipeline'
        ]
        
        stats = {}
        for table in tables:
            try:
                count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", conn).iloc[0]['cnt']
                stats[table] = int(count)
            except:
                stats[table] = 0
        
        conn.close()
        return stats
