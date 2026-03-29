"""
Integration tests for the full pipeline.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from src.orchestrator import PipelineOrchestrator
from config import WAREHOUSE_DB, REPORTS_DIR


class TestPipelineIntegration:
    """End-to-end integration tests."""
    
    def test_full_pipeline_runs(self):
        """Test that the full pipeline executes successfully."""
        orchestrator = PipelineOrchestrator()
        
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        manifest = orchestrator.run_pipeline(start, end)
        
        assert manifest["status"] == "SUCCESS"
        assert len(manifest["stages"]) > 0
    
    def test_warehouse_tables_created(self):
        """Test that warehouse tables are populated."""
        import sqlite3
        
        # Run pipeline first
        orchestrator = PipelineOrchestrator()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        orchestrator.run_pipeline(start, end)
        
        # Check warehouse
        conn = sqlite3.connect(WAREHOUSE_DB)
        cursor = conn.cursor()
        
        tables = [
            "dim_accounts", "dim_contacts",
            "fact_ad_performance", "fact_web_sessions", "fact_email_engagement"
        ]
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            assert count > 0, f"Table {table} should have data"
        
        conn.close()
    
    def test_reports_generated(self):
        """Test that reports are generated."""
        orchestrator = PipelineOrchestrator()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        manifest = orchestrator.run_pipeline(start, end)
        
        # Check that reports were generated
        report_files = list(REPORTS_DIR.glob("*.md"))
        assert len(report_files) > 0, "At least one report should be generated"
    
    def test_manifest_created(self):
        """Test that execution manifest is created."""
        orchestrator = PipelineOrchestrator()
        start = datetime.now() - timedelta(days=30)
        end = datetime.now()
        
        manifest = orchestrator.run_pipeline(start, end)
        
        assert "run_id" in manifest
        assert "stages" in manifest
        assert "duration_seconds" in manifest
