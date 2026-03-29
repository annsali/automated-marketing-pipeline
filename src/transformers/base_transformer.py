"""
Base Transformer Class
Abstract base class for all data transformers.
"""

import logging
import hashlib
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np


class BaseTransformer(ABC):
    """Abstract base class for all data transformers."""
    
    def __init__(self, transformer_name: str, config: Dict[str, Any] = None):
        self.transformer_name = transformer_name
        self.config = config or {}
        self.logger = logging.getLogger(f"transformer.{transformer_name}")
        self.transformation_metadata: Dict[str, Any] = {
            "rows_in": 0,
            "rows_out": 0,
            "columns_added": [],
            "columns_removed": [],
            "transformations": [],
        }
    
    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the input data.
        
        Args:
            data: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get transformation metadata."""
        return {
            "transformer": self.transformer_name,
            **self.transformation_metadata,
        }
    
    def _update_metadata(self, rows_in: int, rows_out: int, **kwargs):
        """Update transformation metadata."""
        self.transformation_metadata.update({
            "rows_in": rows_in,
            "rows_out": rows_out,
            **kwargs,
        })
    
    @staticmethod
    def to_snake_case(name: str) -> str:
        """Convert column name to snake_case."""
        import re
        # Replace spaces and hyphens with underscores
        name = re.sub(r'[\s\-]+', '_', name)
        # Insert underscore between a lowercase/digit and an uppercase letter (camelCase)
        name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
        # Insert underscore between consecutive uppercase letters followed by lowercase (e.g. HTMLParser -> HTML_Parser)
        name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
        # Lowercase everything
        name = name.lower()
        # Remove duplicate underscores
        name = re.sub(r'_+', '_', name)
        return name.strip('_')
    
    @staticmethod
    def normalize_nulls(value):
        """Normalize null-like values to None/NaN."""
        if pd.isna(value):
            return None
        if isinstance(value, str):
            value = value.strip()
            if value.lower() in ('', 'null', 'none', 'n/a', 'na', 'nan', '#n/a'):
                return None
        return value
    
    @staticmethod
    def calculate_hash(row: pd.Series, columns: List[str]) -> str:
        """Calculate hash for deduplication."""
        values = [str(row.get(col, '')) for col in columns]
        hash_string = '|'.join(values)
        return hashlib.md5(hash_string.encode()).hexdigest()
