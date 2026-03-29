"""
Pipeline Orchestrator
Coordinates the full pipeline execution.
"""

import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd

from config import PIPELINE_CONFIG, get_default_date_range
from src.logger import setup_logging, get_logger
from src.alerting import AlertManager
from src.extractors import CRMExtractor, MetaAdsExtractor, GoogleAdsExtractor, GA4Extractor, EmailPlatformExtractor
from src.transformers import SchemaStandardizer, CRMTransformer, AdsTransformer, WebTransformer, EmailTransformer, IdentityResolver
from src.loaders import WarehouseLoader
from src.quality import DQEngine, AnomalyDetector, DQReporter
from src.reporting import DailyReport, WeeklyReport, MonthlyReport


class PipelineOrchestrator:
    """Orchestrates the full marketing data pipeline."""
    
    def __init__(self):
        self.logger = get_logger("orchestrator")
        self.alert_manager = AlertManager()
        self.manifest: Dict[str, Any] = {
            "run_id": f"RUN-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
            "started_at": datetime.utcnow().isoformat(),
            "stages": [],
            "status": "RUNNING",
        }
        self.extracted_data: Dict[str, pd.DataFrame] = {}
        self.transformed_data: Dict[str, pd.DataFrame] = {}
        
    def run_pipeline(self, start_date: datetime = None, end_date: datetime = None) -> Dict[str, Any]:
        """
        Run the complete pipeline.
        
        Args:
            start_date: Start date for extraction
            end_date: End date for extraction
            
        Returns:
            Pipeline execution manifest
        """
        if start_date is None or end_date is None:
            start_date, end_date = get_default_date_range()
        
        self.logger.info(f"Starting pipeline run: {self.manifest['run_id']}")
        self.logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
        
        try:
            # Stage 1: Extract
            self._run_stage("extract", self._stage_extract, start_date, end_date)
            
            # Stage 2: Transform
            self._run_stage("transform", self._stage_transform)
            
            # Stage 3: Resolve Identity
            self._run_stage("identity_resolution", self._stage_identity_resolution)
            
            # Stage 4: Load
            self._run_stage("load", self._stage_load)
            
            # Stage 5: Validate DQ
            dq_results = self._run_stage("dq_validation", self._stage_dq_validation)
            
            # Stage 6: Detect Anomalies
            anomalies = self._run_stage("anomaly_detection", self._stage_anomaly_detection)
            
            # Stage 7: Generate Reports
            reports = self._run_stage("reporting", self._stage_reporting)
            
            # Complete
            self.manifest["status"] = "SUCCESS"
            self.manifest["dq_score"] = dq_results.get("dq_score", 0) if isinstance(dq_results, dict) else 0
            self.manifest["anomalies_detected"] = len(anomalies) if isinstance(anomalies, list) else 0
            self.manifest["reports_generated"] = len(reports) if isinstance(reports, list) else 0
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            self.manifest["status"] = "FAILED"
            self.manifest["error"] = str(e)
            self.alert_manager.pipeline_failure("pipeline", str(e), self.manifest["run_id"])
        
        finally:
            self.manifest["completed_at"] = datetime.utcnow().isoformat()
            self.manifest["duration_seconds"] = (
                datetime.fromisoformat(self.manifest["completed_at"]) - 
                datetime.fromisoformat(self.manifest["started_at"])
            ).total_seconds()
            
            # Save manifest
            self._save_manifest()
        
        return self.manifest
    
    def _run_stage(self, stage_name: str, stage_func, *args, **kwargs):
        """Run a pipeline stage with tracking."""
        self.logger.info(f"Starting stage: {stage_name}")
        stage_start = time.time()
        
        try:
            result = stage_func(*args, **kwargs)
            
            stage_record = {
                "stage": stage_name,
                "status": "SUCCESS",
                "duration_seconds": time.time() - stage_start,
                "timestamp": datetime.utcnow().isoformat(),
            }
            
            # Add result info if applicable
            if isinstance(result, pd.DataFrame):
                stage_record["rows"] = len(result)
            elif isinstance(result, dict):
                stage_record["rows"] = result.get("row_count", 0)
            
            self.manifest["stages"].append(stage_record)
            self.logger.info(f"Stage {stage_name} completed in {stage_record['duration_seconds']:.2f}s")
            
            return result
            
        except Exception as e:
            stage_record = {
                "stage": stage_name,
                "status": "FAILED",
                "error": str(e),
                "duration_seconds": time.time() - stage_start,
                "timestamp": datetime.utcnow().isoformat(),
            }
            self.manifest["stages"].append(stage_record)
            self.logger.error(f"Stage {stage_name} failed: {e}")
            raise
    
    def _stage_extract(self, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        """Extract data from all sources."""
        data = {}
        
        # CRM
        try:
            crm_ext = CRMExtractor()
            data["crm"] = crm_ext.extract_with_retry(start_date, end_date)
            self.extracted_data["crm_accounts"] = crm_ext.accounts_df if hasattr(crm_ext, 'accounts_df') else None
            self.extracted_data["crm_contacts"] = crm_ext.contacts_df if hasattr(crm_ext, 'contacts_df') else None
            self.extracted_data["crm_opportunities"] = crm_ext.opportunities_df if hasattr(crm_ext, 'opportunities_df') else None
            self.extracted_data["crm_activities"] = crm_ext.activities_df if hasattr(crm_ext, 'activities_df') else None
        except Exception as e:
            self.logger.error(f"CRM extraction failed: {e}")
        
        # Meta Ads
        try:
            meta_ext = MetaAdsExtractor()
            data["meta_ads"] = meta_ext.extract_with_retry(start_date, end_date)
        except Exception as e:
            self.logger.error(f"Meta Ads extraction failed: {e}")
        
        # Google Ads
        try:
            google_ext = GoogleAdsExtractor()
            data["google_ads"] = google_ext.extract_with_retry(start_date, end_date)
        except Exception as e:
            self.logger.error(f"Google Ads extraction failed: {e}")
        
        # GA4
        try:
            ga4_ext = GA4Extractor()
            data["ga4"] = ga4_ext.extract_with_retry(start_date, end_date)
            self.extracted_data["ga4_sessions"] = ga4_ext.sessions_df if hasattr(ga4_ext, 'sessions_df') else None
            self.extracted_data["ga4_events"] = ga4_ext.events_df if hasattr(ga4_ext, 'events_df') else None
        except Exception as e:
            self.logger.error(f"GA4 extraction failed: {e}")
        
        # Email
        try:
            email_ext = EmailPlatformExtractor()
            data["email"] = email_ext.extract_with_retry(start_date, end_date)
        except Exception as e:
            self.logger.error(f"Email extraction failed: {e}")
        
        self.extracted_data.update(data)
        return data
    
    def _stage_transform(self) -> Dict[str, pd.DataFrame]:
        """Transform all extracted data."""
        standardizer = SchemaStandardizer()
        
        # Transform CRM
        if self.extracted_data.get("crm_contacts") is not None:
            crm_transformer = CRMTransformer()
            self.transformed_data["crm_contacts"] = crm_transformer.transform(
                standardizer.transform(self.extracted_data["crm_contacts"])
            )
        
        # Transform Ads
        if self.extracted_data.get("meta_ads") is not None and self.extracted_data.get("google_ads") is not None:
            ads_transformer = AdsTransformer()
            self.transformed_data["unified_ads"] = ads_transformer.transform(
                standardizer.transform(self.extracted_data["meta_ads"]),
                standardizer.transform(self.extracted_data["google_ads"])
            )
        
        # Transform Web
        if self.extracted_data.get("ga4_sessions") is not None:
            web_transformer = WebTransformer()
            self.transformed_data["web_sessions"] = web_transformer.transform(
                standardizer.transform(self.extracted_data["ga4_sessions"])
            )
        
        # Transform Email
        if self.extracted_data.get("email") is not None:
            email_transformer = EmailTransformer()
            self.transformed_data["email_engagement"] = email_transformer.transform(
                standardizer.transform(self.extracted_data["email"])
            )
        
        return self.transformed_data
    
    def _stage_identity_resolution(self) -> pd.DataFrame:
        """Run identity resolution."""
        resolver = IdentityResolver()
        
        identity_graph = resolver.transform(
            crm_contacts=self.transformed_data.get("crm_contacts"),
            ga4_sessions=self.transformed_data.get("web_sessions"),
            email_recipients=self.transformed_data.get("email_engagement"),
        )
        
        self.transformed_data["identity_graph"] = identity_graph
        return identity_graph
    
    def _stage_load(self) -> int:
        """Load data into warehouse."""
        loader = WarehouseLoader()
        loader.create_tables()

        total_rows = 0

        # Load dimension tables
        if self.extracted_data.get("crm_accounts") is not None:
            total_rows += loader.load_dataframe(
                self.extracted_data["crm_accounts"], "dim_accounts", if_exists="replace"
            )

        if self.transformed_data.get("crm_contacts") is not None:
            total_rows += loader.load_dataframe(
                self.transformed_data["crm_contacts"], "dim_contacts", if_exists="replace"
            )

        # Load fact tables
        if self.transformed_data.get("unified_ads") is not None:
            total_rows += loader.load_dataframe(
                self.transformed_data["unified_ads"], "fact_ad_performance", if_exists="replace"
            )

        if self.transformed_data.get("web_sessions") is not None:
            total_rows += loader.load_dataframe(
                self.transformed_data["web_sessions"], "fact_web_sessions", if_exists="replace"
            )

        if self.transformed_data.get("email_engagement") is not None:
            email_df = self.transformed_data["email_engagement"].copy()
            # Add date column from sent_at if missing
            if "date" not in email_df.columns and "sent_at" in email_df.columns:
                email_df["date"] = pd.to_datetime(email_df["sent_at"]).dt.strftime("%Y-%m-%d")
            total_rows += loader.load_dataframe(
                email_df, "fact_email_engagement", if_exists="replace"
            )

        if self.extracted_data.get("crm_activities") is not None:
            activities_df = self.extracted_data["crm_activities"].copy()
            # Rename 'type' to 'activity_type' to match DDL
            if "type" in activities_df.columns and "activity_type" not in activities_df.columns:
                activities_df = activities_df.rename(columns={"type": "activity_type"})
            total_rows += loader.load_dataframe(
                activities_df, "fact_crm_activities", if_exists="replace"
            )

        if self.extracted_data.get("crm_opportunities") is not None:
            opps_df = self.extracted_data["crm_opportunities"].copy()
            # Add date column from created_date if missing
            if "date" not in opps_df.columns and "created_date" in opps_df.columns:
                opps_df["date"] = opps_df["created_date"]
            # Calculate sales_cycle_days
            if "sales_cycle_days" not in opps_df.columns:
                opps_df["sales_cycle_days"] = (
                    pd.to_datetime(opps_df["close_date"]) - pd.to_datetime(opps_df["created_date"])
                ).dt.days
            total_rows += loader.load_dataframe(
                opps_df, "fact_pipeline", if_exists="replace"
            )

        return total_rows
    
    def _stage_dq_validation(self) -> Dict[str, Any]:
        """Run data quality validation."""
        dq_engine = DQEngine()
        
        tables = {
            "fact_ad_performance": self.transformed_data.get("unified_ads"),
            "fact_web_sessions": self.transformed_data.get("web_sessions"),
            "fact_email_engagement": self.transformed_data.get("email_engagement"),
        }
        
        # Remove None values
        tables = {k: v for k, v in tables.items() if v is not None}
        
        results = dq_engine.run_checks(tables)
        
        # Generate report
        reporter = DQReporter()
        report_path = reporter.generate_report(results)
        self.logger.info(f"DQ report saved to {report_path}")
        
        # Alert if needed
        if dq_engine.should_halt_pipeline():
            self.alert_manager.dq_failure(
                results["dq_score"],
                [r["check_name"] for r in results["results"] if r["status"] == "FAIL"],
                self.manifest["run_id"]
            )
        
        return results
    
    def _stage_anomaly_detection(self) -> List[Dict]:
        """Run anomaly detection."""
        detector = AnomalyDetector()
        all_anomalies = []
        
        # Check ad performance
        if self.transformed_data.get("unified_ads") is not None:
            ads_df = self.transformed_data["unified_ads"]
            anomalies = detector.detect_all(
                ads_df,
                metrics=["spend", "clicks", "conversions"],
                date_column="date",
                groupby_column="platform"
            )
            all_anomalies.extend(anomalies)
        
        # Check web traffic
        if self.transformed_data.get("web_sessions") is not None:
            web_df = self.transformed_data["web_sessions"]
            # Aggregate by date
            daily_sessions = web_df.groupby("date").size().reset_index(name="sessions")
            anomalies = detector.detect_all(
                daily_sessions,
                metrics=["sessions"],
                date_column="date"
            )
            all_anomalies.extend(anomalies)
        
        # Alert on critical anomalies
        critical_anomalies = [a for a in all_anomalies if a["severity"] == "CRITICAL"]
        for anomaly in critical_anomalies[:3]:  # Alert on first 3
            self.alert_manager.critical_anomaly(
                anomaly["metric"],
                anomaly["observed_value"],
                anomaly.get("expected_value", 0),
                self.manifest["run_id"]
            )
        
        return all_anomalies
    
    def _stage_reporting(self) -> List[Dict]:
        """Generate reports."""
        reports = []
        
        # Daily report (always)
        daily = DailyReport()
        reports.append(daily.generate())
        
        # Weekly report (on Mondays)
        if datetime.now().weekday() == 0:
            weekly = WeeklyReport()
            reports.append(weekly.generate())
        
        # Monthly report (on 1st of month)
        if datetime.now().day == 1:
            monthly = MonthlyReport()
            reports.append(monthly.generate())
        
        return reports
    
    def _save_manifest(self):
        """Save pipeline execution manifest."""
        manifest_path = Path(PIPELINE_CONFIG["manifest_file"])
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(manifest_path, "w") as f:
            json.dump(self.manifest, f, indent=2)
        
        self.logger.info(f"Pipeline manifest saved to {manifest_path}")
