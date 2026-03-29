# Automated Marketing Data Pipeline & Reporting Engine

> Production-grade ETL pipeline integrating CRM, Meta Ads, Google Ads, GA4, and email platform data into a unified marketing data warehouse with automated quality validation, anomaly detection, and scheduled performance reporting.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![SQL](https://img.shields.io/badge/SQL-SQLite-green)
![ETL](https://img.shields.io/badge/Pipeline-ETL-orange)
![License](https://img.shields.io/badge/License-MIT-yellow)

## Overview

Marketing teams drown in fragmented data across platforms. This pipeline solves that by extracting from 5 sources, transforming into a unified data model, validating quality, detecting anomalies, and generating automated reports — the same infrastructure that powers marketing analytics at companies like Meta.

**Author:** Anns Ali Syed

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         MARKETING DATA PIPELINE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  SOURCES            EXTRACT           TRANSFORM          WAREHOUSE      │
│  ┌─────────┐       ┌─────────┐       ┌─────────┐       ┌─────────┐     │
│  │   CRM   │──────▶│  Base   │──────▶│ Schema  │──────▶│  Star   │     │
│  │(Salesfo │       │Extractor│       │Standard │       │ Schema  │     │
│  └─────────┘       └─────────┘       └─────────┘       │    ★    │     │
│  ┌─────────┐       ┌─────────┐       ┌─────────┐       │         │     │
│  │  Meta   │──────▶│  Meta   │──────▶│   Ads   │──────▶│5 Facts  │     │
│  │  Ads    │       │Extractor│       │Transform│       │4 Dims   │     │
│  └─────────┘       └─────────┘       └─────────┘       └────┬────┘     │
│  ┌─────────┐       ┌─────────┐       ┌─────────┐            │          │
│  │  Google │──────▶│  Google │──────▶│Identity │            │          │
│  │  Ads    │       │Extractor│       │Resolver │            │          │
│  └─────────┘       └─────────┘       └─────────┘            │          │
│  ┌─────────┐       ┌─────────┐       ┌─────────┐            │          │
│  │   GA4   │──────▶│   GA4   │──────▶│   Web   │            │          │
│  │(Web)    │       │Extractor│       │Transform│            │          │
│  └─────────┘       └─────────┘       └─────────┘            │          │
│  ┌─────────┐       ┌─────────┐       ┌─────────┐            │          │
│  │  Email  │──────▶│  Email  │──────▶│  Email  │            │          │
│  │ (SFMC)  │       │Extractor│       │Transform│            │          │
│  └─────────┘       └─────────┘       └─────────┘            │          │
│                                                             ▼          │
│                    QUALITY              REPORTING                       │
│                   ┌─────────┐          ┌─────────┐                     │
│                   │   DQ    │          │  Daily  │                     │
│                   │ Engine  │          │ Report  │                     │
│                   │ (12 chk)│          ├─────────┤                     │
│                   ├─────────┤          │ Weekly  │                     │
│                   │ Anomaly │          │ Report  │                     │
│                   │Detection│          ├─────────┤                     │
│                   │(3 meth) │          │ Monthly │                     │
│                   └─────────┘          │ Report  │                     │
│                                        └─────────┘                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Key Features

- **5 data source extractors** with retry logic, validation, and metadata tracking
- **Cross-platform identity resolution** matching users across CRM, web, ads, and email
- **Star schema data warehouse** with fact/dimension tables and incremental loading
- **12 automated DQ checks** with quality scoring and severity classification
- **Statistical anomaly detection** using Z-score, IQR, and percentage change methods
- **3 report cadences** (daily pulse, weekly summary, monthly executive) with Jinja2 templates
- **Pipeline orchestration** with stage tracking, retry logic, checkpointing, and alerting
- **4.15M+ total records** processed across all sources

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd marketing-data-pipeline

# Install dependencies
pip install -r requirements.txt
```

### Run the Pipeline

```bash
# Run with default date range (last 18 months)
python run_pipeline.py

# Run with specific date range
python run_pipeline.py --start-date 2024-01-01 --end-date 2024-06-30
```

### Run Tests

```bash
pytest tests/ -v
```

## Data Sources

| Source | Records | Description |
|--------|---------|-------------|
| CRM (Salesforce) | 335K | Accounts, Contacts, Opportunities, Activities |
| Meta Ads | 250K | Daily campaign performance across 40 campaigns |
| Google Ads | 200K | Daily campaign performance across 30 campaigns |
| GA4 (Web) | 2.5M | Sessions and events |
| Email (SFMC) | 860K | Campaign sends and engagement |

## Data Model

### Fact Tables

- **fact_ad_performance** — Daily ad metrics by campaign/ad set/ad
- **fact_web_sessions** — Session-level web analytics
- **fact_email_engagement** — Recipient-level email metrics
- **fact_crm_activities** — CRM activity log
- **fact_pipeline** — Opportunity/pipeline metrics

### Dimension Tables

- **dim_accounts** — Account master
- **dim_contacts** — Contact master with unified identity
- **dim_campaigns** — Campaign master across all platforms
- **dim_dates** — Date dimension

## Data Quality Framework

### Automated Checks

| Check | Severity | Description |
|-------|----------|-------------|
| Completeness | CRITICAL | % of NULL values per column |
| Uniqueness | CRITICAL | Duplicate primary keys |
| Freshness | HIGH | Most recent record age |
| Volume | HIGH | Row count vs historical average |
| Schema | CRITICAL | Expected columns present |
| Value Range | MEDIUM | Numeric values within bounds |
| Email Validity | LOW | Email format validation |

### Scoring

- **PASS**: Score ≥ 90
- **WARNING**: Score 70-89
- **FAIL**: Score < 70

Critical failures automatically halt downstream processing.

## Anomaly Detection

Three statistical methods for detecting data anomalies:

1. **Z-Score**: Flags values > 3 standard deviations from trailing 30-day mean
2. **IQR**: Flags values outside 1.5× IQR from trailing 90-day data
3. **Percentage Change**: Flags day-over-day changes exceeding 30%

Monitored metrics include ad spend, CTR, conversion volume, email delivery rate, and web traffic.

## Project Structure

```
marketing-data-pipeline/
├── README.md
├── requirements.txt
├── config.py
├── run_pipeline.py
├── data/
│   ├── raw/           # Raw extracted data
│   ├── staging/       # Intermediate transforms
│   └── warehouse/     # SQLite database
├── src/
│   ├── extractors/    # 5 data extractors
│   ├── transformers/  # Data transformation modules
│   ├── loaders/       # Warehouse loading
│   ├── quality/       # DQ framework
│   ├── reporting/     # Report generation
│   ├── orchestrator.py
│   ├── logger.py
│   └── alerting.py
├── sql/               # SQL transforms
├── tests/             # pytest test suite
├── reports/           # Generated reports
└── logs/              # Pipeline logs
```

## Sample Reports

### Daily Marketing Pulse
```
═══════════════════════════════════════════════════
DAILY MARKETING PULSE — October 15, 2025
Data as of: 2025-10-15 08:00 UTC | Pipeline Status: ✅ HEALTHY
═══════════════════════════════════════════════════

SPEND & EFFICIENCY
  Total Spend Today:      $8,420
  Meta Ads:               $5,100
  Google Ads:             $3,320
  Blended CPC:            $2.14
  Blended CPA:            $84.20

TRAFFIC & ENGAGEMENT
  Total Sessions:         12,450
  Paid Sessions:          4,200
  Organic Sessions:       5,800
  Bounce Rate:            42.3%
```

## Tech Stack

- **Python 3.10+**: pandas, numpy, faker, sqlalchemy, scipy
- **SQL**: SQLite with expert-level queries
- **Testing**: pytest
- **Templating**: Jinja2 for report generation
- **Logging**: Structured logging with file/console output

## SQL Showcase

### Unified Ad Performance Query
```sql
WITH daily_performance AS (
    SELECT 
        date,
        platform,
        SUM(spend) as total_spend,
        SUM(conversions) as total_conversions,
        CASE WHEN SUM(spend) > 0 
            THEN SUM(conversion_value) / SUM(spend) 
            ELSE 0 
        END as roas
    FROM fact_ad_performance
    WHERE date >= date('now', '-30 days')
    GROUP BY date, platform
)
SELECT * FROM daily_performance
ORDER BY date DESC, total_spend DESC;
```

### Channel Attribution Query
```sql
SELECT 
    channel,
    SUM(spend) as total_spend,
    SUM(conversion_value) as total_revenue,
    SUM(conversions) as total_conversions,
    CASE WHEN SUM(spend) > 0 
        THEN SUM(conversion_value) / SUM(spend) 
        ELSE 0 
    END as blended_roas
FROM fact_ad_performance
GROUP BY channel
ORDER BY total_revenue DESC;
```



## License

MIT License - See LICENSE file for details.
