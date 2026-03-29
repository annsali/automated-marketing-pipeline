"""
Alerting and Notification System
Simulates email/Slack alerts for pipeline events.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from enum import Enum

from config import ALERT_CONFIG, LOGS_DIR


class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertManager:
    """Manages pipeline alerts and notifications."""
    
    def __init__(self, config: Dict = None):
        self.config = config or ALERT_CONFIG
        self.logger = logging.getLogger("alerting")
        self.alerts_log = LOGS_DIR / "alerts.log"
        self.alerts: List[Dict[str, Any]] = []
        
        # Ensure logs directory exists
        self.alerts_log.parent.mkdir(parents=True, exist_ok=True)
    
    def send_alert(
        self,
        severity: AlertSeverity,
        title: str,
        description: str,
        affected_data: str = None,
        recommendation: str = None,
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Send an alert for a pipeline event.
        
        Args:
            severity: Alert severity level
            title: Short alert title
            description: Detailed description
            affected_data: What data is affected
            recommendation: Suggested action
            context: Additional context data
            
        Returns:
            The alert record that was created
        """
        if not self.config.get("enabled", True):
            self.logger.info(f"Alerting disabled. Would have sent: {title}")
            return None
        
        alert = {
            "alert_id": f"ALT-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
            "timestamp": datetime.utcnow().isoformat(),
            "severity": severity.value,
            "title": title,
            "description": description,
            "affected_data": affected_data,
            "recommendation": recommendation,
            "context": context or {},
        }
        
        self.alerts.append(alert)
        self._log_alert(alert)
        
        # Log at appropriate level
        log_msg = f"[{severity.value.upper()}] {title}: {description}"
        if severity == AlertSeverity.CRITICAL:
            self.logger.critical(log_msg)
        elif severity == AlertSeverity.ERROR:
            self.logger.error(log_msg)
        elif severity == AlertSeverity.WARNING:
            self.logger.warning(log_msg)
        else:
            self.logger.info(log_msg)
        
        return alert
    
    def _log_alert(self, alert: Dict[str, Any]):
        """Write alert to alerts log file."""
        with open(self.alerts_log, "a") as f:
            f.write(json.dumps(alert) + "\n")
    
    def pipeline_failure(
        self,
        stage: str,
        error: str,
        run_id: str = None
    ) -> Dict[str, Any]:
        """Alert for pipeline failure."""
        if not self.config.get("on_pipeline_failure", True):
            return None
            
        return self.send_alert(
            severity=AlertSeverity.CRITICAL,
            title=f"Pipeline Failure: {stage}",
            description=f"Pipeline stage '{stage}' failed with error: {error}",
            affected_data=f"Stage: {stage}",
            recommendation="Check pipeline logs and retry. If persistent, investigate source system.",
            context={"stage": stage, "error": error, "run_id": run_id}
        )
    
    def dq_failure(
        self,
        dq_score: float,
        failures: List[str],
        run_id: str = None
    ) -> Dict[str, Any]:
        """Alert for data quality failure."""
        if not self.config.get("on_dq_fail", True):
            return None
            
        threshold = self.config.get("dq_score_threshold", 70)
        
        return self.send_alert(
            severity=AlertSeverity.ERROR,
            title=f"Data Quality Score Below Threshold: {dq_score:.1f}",
            description=f"DQ score {dq_score:.1f} is below threshold {threshold}. Failures: {', '.join(failures)}",
            affected_data="All downstream reports",
            recommendation="Review DQ report and address data quality issues before proceeding.",
            context={"dq_score": dq_score, "threshold": threshold, "failures": failures, "run_id": run_id}
        )
    
    def critical_anomaly(
        self,
        metric: str,
        observed: float,
        expected: float,
        run_id: str = None
    ) -> Dict[str, Any]:
        """Alert for critical anomaly detection."""
        if not self.config.get("on_critical_anomaly", True):
            return None
            
        pct_diff = ((observed - expected) / expected * 100) if expected else 0
        
        return self.send_alert(
            severity=AlertSeverity.WARNING,
            title=f"Critical Anomaly Detected: {metric}",
            description=f"Metric '{metric}' shows anomalous value {observed:.2f} (expected {expected:.2f}, {pct_diff:+.1f}%)",
            affected_data=f"Metric: {metric}",
            recommendation="Verify data source and investigate any recent changes to campaigns or tracking.",
            context={"metric": metric, "observed": observed, "expected": expected, "pct_diff": pct_diff, "run_id": run_id}
        )
    
    def get_alerts(
        self,
        severity: AlertSeverity = None,
        since: datetime = None
    ) -> List[Dict[str, Any]]:
        """Get filtered alerts."""
        filtered = self.alerts
        
        if severity:
            filtered = [a for a in filtered if a["severity"] == severity.value]
        
        if since:
            since_str = since.isoformat()
            filtered = [a for a in filtered if a["timestamp"] >= since_str]
        
        return filtered
    
    def clear_alerts(self):
        """Clear all alerts."""
        self.alerts = []
