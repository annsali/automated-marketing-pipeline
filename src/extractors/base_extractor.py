"""
Base Extractor Class
Abstract base class for all data extractors.
"""

import logging
import time
import random
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd

from config import RAW_DIR, PIPELINE_CONFIG


class BaseExtractor(ABC):
    """Abstract base class for all data extractors."""
    
    def __init__(self, source_name: str, config: Dict[str, Any]):
        self.source_name = source_name
        self.config = config
        self.logger = logging.getLogger(f"extractor.{source_name}")
        self.extraction_metadata: Dict[str, Any] = {}
        self.raw_data_dir = RAW_DIR / source_name
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def extract(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Extract data from the source.
        
        Args:
            start_date: Start of extraction period
            end_date: End of extraction period
            
        Returns:
            DataFrame with extracted data
        """
        pass
    
    @abstractmethod
    def validate_response(self, data: pd.DataFrame) -> bool:
        """
        Validate the extracted data.
        
        Args:
            data: Extracted DataFrame
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get extraction metadata."""
        return {
            "source": self.source_name,
            "extracted_at": datetime.utcnow().isoformat(),
            "row_count": self.extraction_metadata.get("row_count", 0),
            "date_range": self.extraction_metadata.get("date_range", {}),
            "schema_version": self.config.get("schema_version", "1.0"),
            "status": self.extraction_metadata.get("status", "unknown"),
        }
    
    def _simulate_failure(self) -> bool:
        """Simulate random API failures based on config."""
        failure_rate = self.config.get("failure_rate", 0.05)
        return random.random() < failure_rate
    
    def _save_raw_data(self, data: pd.DataFrame, date_str: str):
        """Save raw data to file."""
        date_dir = self.raw_data_dir / date_str
        date_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = date_dir / f"{self.source_name}_data.csv"
        data.to_csv(file_path, index=False)
        self.logger.info(f"Raw data saved to {file_path}")
    
    def extract_with_retry(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Extract data with retry logic.
        
        Args:
            start_date: Start of extraction period
            end_date: End of extraction period
            
        Returns:
            DataFrame with extracted data
        """
        max_retries = PIPELINE_CONFIG["max_retries"]
        retry_delay_base = PIPELINE_CONFIG["retry_delay_base"]
        retry_delay_max = PIPELINE_CONFIG["retry_delay_max"]
        
        for attempt in range(max_retries):
            try:
                self.logger.info(
                    f"Extraction attempt {attempt + 1}/{max_retries} for {self.source_name}"
                )
                
                # Simulate potential API failure
                if self._simulate_failure():
                    raise Exception(f"Simulated API timeout for {self.source_name}")
                
                # Perform extraction
                start_time = time.time()
                data = self.extract(start_date, end_date)
                extraction_time = time.time() - start_time
                
                # Validate response
                if not self.validate_response(data):
                    raise Exception(f"Validation failed for {self.source_name}")
                
                # Update metadata
                self.extraction_metadata.update({
                    "row_count": len(data),
                    "date_range": {
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat(),
                    },
                    "status": "success",
                    "extraction_time_seconds": extraction_time,
                    "attempts": attempt + 1,
                })
                
                # Save raw data
                date_str = datetime.now().strftime("%Y%m%d")
                self._save_raw_data(data, date_str)
                
                self.logger.info(
                    f"Successfully extracted {len(data)} rows from {self.source_name} "
                    f"in {extraction_time:.2f}s"
                )
                
                return data
                
            except Exception as e:
                self.logger.warning(f"Extraction attempt {attempt + 1} failed: {e}")
                
                if attempt < max_retries - 1:
                    # Exponential backoff
                    delay = min(retry_delay_base ** (attempt + 1), retry_delay_max)
                    self.logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    self.extraction_metadata.update({
                        "status": "failed",
                        "error": str(e),
                        "attempts": attempt + 1,
                    })
                    self.logger.error(f"All {max_retries} extraction attempts failed")
                    raise
        
        return pd.DataFrame()  # Should not reach here
