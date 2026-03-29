"""
Schema Standardizer
Universal schema standardization for all data sources.
"""

import re
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any

from .base_transformer import BaseTransformer
from config import CURRENCY_RATES


class SchemaStandardizer(BaseTransformer):
    """Standardizes schema across all data sources."""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("schema_standardizer", config)
    
    def transform(self, data: pd.DataFrame, source_type: str = None) -> pd.DataFrame:
        """
        Standardize schema of input data.
        
        Args:
            data: Input DataFrame
            source_type: Type of data source (for source-specific handling)
            
        Returns:
            Standardized DataFrame
        """
        rows_in = len(data)
        self.logger.info(f"Standardizing schema for {rows_in} rows")
        
        df = data.copy()
        columns_added = []
        columns_removed = []
        transformations = []
        
        # 1. Column naming: convert to snake_case, lowercase
        original_cols = list(df.columns)
        df.columns = [self.to_snake_case(col).lower() for col in df.columns]
        if original_cols != list(df.columns):
            transformations.append("converted_columns_to_snake_case")
        
        # 2. Date handling: standardize date columns
        date_cols = [col for col in df.columns if any(x in col for x in ['date', 'timestamp', '_at', 'time'])]
        for col in date_cols:
            if col in df.columns:
                df[col] = self._standardize_dates(df[col])
        if date_cols:
            transformations.append(f"standardized_dates: {date_cols}")
        
        # 3. Null handling
        df = df.map(self.normalize_nulls)
        transformations.append("normalized_null_values")
        
        # 4. String normalization
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            df[col] = self._normalize_strings(df[col])
        transformations.append("normalized_strings")
        
        # 5. Email normalization
        email_cols = [col for col in df.columns if 'email' in col]
        for col in email_cols:
            if col in df.columns:
                df[col] = df[col].str.lower().str.strip() if df[col].dtype == 'object' else df[col]
        if email_cols:
            transformations.append(f"normalized_emails: {email_cols}")
        
        # 6. Currency conversion
        currency_cols = [col for col in df.columns if any(x in col for x in ['cost', 'spend', 'revenue', 'value', 'amount'])]
        # Note: In real implementation, would check currency column
        for col in currency_cols:
            if col in df.columns and df[col].dtype in ['int64', 'float64']:
                df[col] = df[col].apply(lambda x: x * CURRENCY_RATES.get('USD', 1.0) if pd.notna(x) else x)
        if currency_cols:
            transformations.append(f"normalized_currency: {currency_cols}")
        
        # 7. Deduplication
        rows_before = len(df)
        df = df.drop_duplicates()
        rows_after = len(df)
        if rows_before > rows_after:
            transformations.append(f"removed_duplicates: {rows_before - rows_after} rows")
        
        # Update metadata
        self._update_metadata(
            rows_in=rows_in,
            rows_out=len(df),
            columns_added=columns_added,
            columns_removed=columns_removed,
            transformations=transformations,
        )
        
        self.logger.info(f"Schema standardization complete: {len(df)} rows output")
        return df
    
    def _standardize_dates(self, series: pd.Series) -> pd.Series:
        """Standardize date format to YYYY-MM-DD or datetime."""
        def parse_date(val):
            if pd.isna(val):
                return None
            if isinstance(val, datetime):
                return val.strftime("%Y-%m-%d")
            if isinstance(val, str):
                for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"]:
                    try:
                        dt = datetime.strptime(val.strip(), fmt)
                        return dt.strftime("%Y-%m-%d")
                    except ValueError:
                        continue
            return val
        
        return series.apply(parse_date)
    
    def _normalize_strings(self, series: pd.Series) -> pd.Series:
        """Normalize string values."""
        if series.dtype != 'object':
            return series
        
        return series.apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    def _detect_type(self, series: pd.Series) -> str:
        """Detect the data type of a series."""
        if pd.api.types.is_datetime64_any_dtype(series):
            return 'datetime'
        elif pd.api.types.is_integer_dtype(series):
            return 'integer'
        elif pd.api.types.is_float_dtype(series):
            return 'float'
        elif pd.api.types.is_bool_dtype(series):
            return 'boolean'
        else:
            return 'string'
