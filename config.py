"""
Pipeline Configuration
Centralized configuration for the Marketing Data Pipeline.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
REPORTS_DIR = BASE_DIR / "reports"
LOGS_DIR = BASE_DIR / "logs"
SQL_DIR = BASE_DIR / "sql"

# Ensure directories exist
for dir_path in [RAW_DIR, STAGING_DIR, WAREHOUSE_DIR, REPORTS_DIR, LOGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Database
WAREHOUSE_DB = WAREHOUSE_DIR / "marketing_warehouse.db"

# Pipeline settings
PIPELINE_CONFIG = {
    "max_retries": 3,
    "retry_delay_base": 2,  # seconds
    "retry_delay_max": 30,  # seconds
    "batch_size": 10000,
    "checkpoint_enabled": True,
    "manifest_file": DATA_DIR / "pipeline_manifest.json",
}

# Data quality thresholds
DQ_THRESHOLDS = {
    "completeness_min": 0.95,
    "freshness_hours": 24,
    "volume_tolerance": 0.50,  # ±50% from historical average
    "uniqueness_min": 1.0,  # 100% unique required
    "email_validity_min": 0.90,
}

# Anomaly detection settings
ANOMALY_CONFIG = {
    "z_score_threshold": 3.0,
    "iqr_multiplier": 1.5,
    "trailing_days": 30,
    "pct_change_threshold": 0.30,  # 30% day-over-day
    "min_data_points": 7,
}

# Source configurations
SOURCES = {
    "crm": {
        "name": "salesforce_crm",
        "schema_version": "1.0",
        "accounts_count": 30000,
        "contacts_count": 90000,
        "opportunities_count": 15000,
        "activities_count": 200000,
        "failure_rate": 0.05,
    },
    "meta_ads": {
        "name": "meta_ads",
        "schema_version": "1.0",
        "campaigns": 10,
        "date_range_months": 18,
        "failure_rate": 0.05,
    },
    "google_ads": {
        "name": "google_ads",
        "schema_version": "1.0",
        "campaigns": 8,
        "date_range_months": 18,
        "failure_rate": 0.05,
    },
    "ga4": {
        "name": "google_analytics_4",
        "schema_version": "1.0",
        "sessions_count": 500000,
        "events_count": 2000000,
        "date_range_months": 18,
        "failure_rate": 0.05,
    },
    "email": {
        "name": "sfmc_email",
        "schema_version": "1.0",
        "campaigns": 60,
        "sends_count": 800000,
        "date_range_months": 18,
        "failure_rate": 0.05,
    },
}

# Identity resolution settings
IDENTITY_CONFIG = {
    "exact_email_weight": 1.0,
    "campaign_fuzzy_weight": 0.7,
    "timestamp_proximity_weight": 0.5,
    "min_confidence_threshold": 0.5,
    "target_match_rate": 0.80,
}

# Report schedules
REPORT_SCHEDULES = {
    "daily": {
        "enabled": True,
        "time": "08:00",
        "days": ["monday", "tuesday", "wednesday", "thursday", "friday"],
    },
    "weekly": {
        "enabled": True,
        "day": "monday",
        "time": "09:00",
    },
    "monthly": {
        "enabled": True,
        "day": 1,  # First of month
        "time": "10:00",
    },
}

# Alert settings
ALERT_CONFIG = {
    "enabled": True,
    "on_pipeline_failure": True,
    "on_dq_fail": True,
    "on_critical_anomaly": True,
    "dq_score_threshold": 70,
}

# Date ranges for extraction
def get_default_date_range():
    """Get default 18-month date range for extraction."""
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=18*30)
    return start_date, end_date

# Logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
        "detailed": {
            "format": "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d: %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "filename": str(LOGS_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
            "mode": "a",
        },
    },
    "root": {
        "level": "DEBUG",
        "handlers": ["console", "file"],
    },
}

# Currency conversion rates (to USD)
CURRENCY_RATES = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
}

# Campaign objective mapping
CAMPAIGN_OBJECTIVES = {
    "CONVERSIONS": "conversions",
    "TRAFFIC": "traffic",
    "LEAD_GENERATION": "lead_gen",
    "AWARENESS": "awareness",
    "SEARCH": "search",
    "DISPLAY": "display",
    "VIDEO": "video",
    "PERFORMANCE_MAX": "pmax",
}

# Channel grouping rules
CHANNEL_GROUPING = {
    "paid_search": {
        "sources": ["google", "bing"],
        "mediums": ["cpc", "ppc", "paid"],
    },
    "paid_social": {
        "sources": ["facebook", "instagram", "meta", "linkedin", "twitter"],
        "mediums": ["cpc", "paid", "social"],
    },
    "organic_search": {
        "sources": ["google", "bing", "yahoo", "duckduckgo"],
        "mediums": ["organic"],
    },
    "email": {
        "sources": ["email", "sfmc", "mailchimp"],
        "mediums": ["email"],
    },
    "direct": {
        "sources": ["direct", "(direct)"],
        "mediums": ["none", "(none)"],
    },
    "referral": {
        "mediums": ["referral"],
    },
    "display": {
        "mediums": ["display", "banner", "cpm"],
    },
}
