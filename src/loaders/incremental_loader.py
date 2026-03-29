"""
Incremental Loader
Handles incremental/delta loads with upsert logic.
"""

import hashlib
import sqlite3
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Tuple

from .warehouse_loader import WarehouseLoader


class IncrementalLoader(WarehouseLoader):
    """Loader for incremental data loads."""
    
    def __init__(self, db_path=None):
        super().__init__(db_path)
        self.logger = logging.getLogger("incremental_loader")
    
    def calculate_row_hash(self, row: pd.Series, key_columns: List[str]) -> str:
        """Calculate hash for change detection."""
        values = [str(row.get(col, '')) for col in key_columns]
        return hashlib.md5('|'.join(values).encode()).hexdigest()
    
    def incremental_load(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: List[str],
        timestamp_column: str = None
    ) -> Dict[str, int]:
        """
        Perform incremental load with upsert logic.
        
        Args:
            df: New data to load
            table_name: Target table
            key_columns: Columns that define uniqueness
            timestamp_column: Column for change tracking
            
        Returns:
            Dict with new_rows, updated_rows, unchanged_rows counts
        """
        if df.empty:
            return {'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0}
        
        conn = sqlite3.connect(self.db_path)
        
        # Check if table exists
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # First load - simple insert
            df['loaded_at'] = datetime.utcnow().isoformat()
            df.to_sql(table_name, conn, index=False)
            conn.close()
            return {'new_rows': len(df), 'updated_rows': 0, 'unchanged_rows': 0}
        
        # Get existing data
        existing = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        
        if existing.empty:
            # Table exists but empty
            df['loaded_at'] = datetime.utcnow().isoformat()
            df.to_sql(table_name, conn, if_exists='append', index=False)
            conn.close()
            return {'new_rows': len(df), 'updated_rows': 0, 'unchanged_rows': 0}
        
        # Calculate hashes for comparison
        df['_hash'] = df.apply(lambda row: self.calculate_row_hash(row, key_columns), axis=1)
        existing['_hash'] = existing.apply(lambda row: self.calculate_row_hash(row, key_columns), axis=1)
        
        # Create composite key for matching
        df['_key'] = df[key_columns].apply(lambda row: '|'.join(row.astype(str)), axis=1)
        existing['_key'] = existing[key_columns].apply(lambda row: '|'.join(row.astype(str)), axis=1)
        
        # Identify new, updated, and unchanged rows
        existing_keys = set(existing['_key'])
        new_mask = ~df['_key'].isin(existing_keys)
        
        potentially_updated = df[~new_mask].copy()
        unchanged_mask = potentially_updated['_key'].isin(
            existing[existing['_hash'].isin(potentially_updated['_hash'])]['_key']
        )
        updated_mask = ~unchanged_mask
        
        new_rows = df[new_mask].copy()
        updated_rows = potentially_updated[updated_mask].copy()
        unchanged_count = unchanged_mask.sum()
        
        # Clean up temp columns
        new_rows = new_rows.drop(columns=['_hash', '_key'])
        updated_rows = updated_rows.drop(columns=['_hash', '_key'])
        
        # Insert new rows
        if not new_rows.empty:
            new_rows['loaded_at'] = datetime.utcnow().isoformat()
            new_rows.to_sql(table_name, conn, if_exists='append', index=False)
        
        # Update existing rows (delete old, insert new for SQLite)
        if not updated_rows.empty:
            # Delete old versions
            for _, row in updated_rows.iterrows():
                where_clause = ' AND '.join([f"{col} = ?" for col in key_columns])
                values = [row[col] for col in key_columns]
                cursor.execute(f"DELETE FROM {table_name} WHERE {where_clause}", values)
            
            # Insert new versions
            updated_rows['loaded_at'] = datetime.utcnow().isoformat()
            updated_rows.to_sql(table_name, conn, if_exists='append', index=False)
        
        conn.commit()
        conn.close()
        
        result = {
            'new_rows': len(new_rows),
            'updated_rows': len(updated_rows),
            'unchanged_rows': unchanged_count
        }
        
        self.logger.info(f"Incremental load complete for {table_name}: {result}")
        return result
    
    def get_load_delta(self, table_name: str, hours: int = 24) -> pd.DataFrame:
        """Get records loaded in the last N hours."""
        conn = sqlite3.connect(self.db_path)
        
        query = f"""
            SELECT * FROM {table_name}
            WHERE loaded_at >= datetime('now', '-{hours} hours')
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        return df


import logging
