"""
SQLite storage implementation.
"""

import sqlite3
from pathlib import Path
from typing import List, Dict, Any # Added Any import back for _sql_type_for_value
from .base_storage import BaseStorage
import logging
from datetime import datetime, date # Added date import back

class SQLiteStorage(BaseStorage):
    """SQLite database storage handler."""

    def __init__(self, config: Dict):
        super().__init__(config)
        # Use job name for db name if available, sanitize it
        job_name = config.get('name', 'scraped_data').replace(' ', '_').lower()
        safe_job_name = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in job_name)
        self.db_name = config.get('db_name', f'{safe_job_name}.db')
        # Sanitize table name as well
        table_name_base = config.get('table_name', 'scraped_items').replace(' ', '_').lower()
        self.table_name = "".join(c if c.isalnum() or c == '_' else '_' for c in table_name_base)


    def save(self, data: List[Dict], filename: str = None) -> str:
        """Save data to SQLite database. Filename argument is ignored."""
        if not data:
            self.logger.warning("No data provided to save to SQLite.")
            return "" # Return empty path if no data

        # Output directory is handled by BaseStorage (__init__)
        # self.output_dir comes from BaseStorage
        db_path = self.output_dir / self.db_name
        conn = None # Initialize conn to None

        try:
            # Ensure the directory exists (handled by BaseStorage, but good practice)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            cursor = conn.cursor()

            # Create table if not exists
            # Use the keys from the first data item for column definition
            sample_item = data[0]
            columns_defs = self._generate_column_definitions(sample_item)

            # Basic check if table structure matches (optional, can be complex)
            # This is a simple check, might need more robust migration logic for production
            try:
                 cursor.execute(f"PRAGMA table_info({self.table_name})")
                 existing_columns = {row[1] for row in cursor.fetchall()}
                 new_columns = set(sample_item.keys())
                 if existing_columns != new_columns:
                     self.logger.warning(f"Table '{self.table_name}' schema mismatch. Recreating table.")
                     cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                     # Fall through to create table again
            except sqlite3.OperationalError:
                 # Table probably doesn't exist, which is fine
                 pass


            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS "{self.table_name}" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {columns_defs},
                scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_sql)

            # Insert data
            # Use dict keys for column names for robustness
            columns = ['"{}"'.format(k) for k in sample_item.keys()] # Quote column names
            placeholders = ', '.join(['?'] * len(columns))
            insert_sql = f'INSERT INTO "{self.table_name}" ({", ".join(columns)}) VALUES ({placeholders})'

            # Prepare data for executemany
            data_to_insert = []
            for item in data:
                 # Ensure item has all keys from sample_item, filling missing with None
                 row_values = [self._prepare_value(item.get(key)) for key in sample_item.keys()]
                 data_to_insert.append(tuple(row_values))


            cursor.executemany(insert_sql, data_to_insert)

            conn.commit()
            self.logger.info(f"Saved {len(data)} items to SQLite table '{self.table_name}' in database: {db_path}")
            return str(db_path)
        except Exception as e:
            self.logger.error(f"Failed to save to SQLite table '{self.table_name}': {e}")
            if conn:
                conn.rollback() # Rollback changes on error
            raise # Re-raise the exception
        finally:
            if conn:
                conn.close()


    def _generate_column_definitions(self, sample_item: Dict) -> str:
        """Generate SQL column definitions (name and type) from data sample."""
        columns = []
        for field, value in sample_item.items():
            sql_type = self._sql_type_for_value(value)
            # Quote field names to handle keywords or special characters
            columns.append(f'"{field}" {sql_type}')
        return ', '.join(columns)

    def _sql_type_for_value(self, value: Any) -> str:
        """Determine appropriate SQLite type for Python value."""
        # Order matters: check bool before int
        if isinstance(value, bool):
            return 'INTEGER' # Store bools as 0 or 1
        elif isinstance(value, int):
            return 'INTEGER'
        elif isinstance(value, float):
            return 'REAL'
        elif isinstance(value, (datetime, date)):
             # Store timestamps/dates as ISO format strings (TEXT is safer across DBs)
             # Or use native TIMESTAMP type if detect_types is enabled
             return 'TIMESTAMP' # Relies on detect_types for conversion
             # return 'TEXT'
        elif isinstance(value, (str, bytes)):
            return 'TEXT'
        elif value is None:
             return 'TEXT' # Default type for None, can be overridden if needed
        else:
            # For lists, dicts, etc., store as JSON strings
            return 'TEXT' # Store complex types as TEXT (e.g., JSON string)


    def _prepare_value(self, value: Any) -> Any:
        """Prepare Python value for SQL insertion."""
        if isinstance(value, (datetime, date)):
            # Let SQLite handle timestamp conversion with detect_types
            return value
            # return value.isoformat() # Alternative: Store as ISO string
        elif isinstance(value, bool):
            return 1 if value else 0 # Store bools as 1 or 0
        elif isinstance(value, (list, dict)):
            # Store lists/dicts as JSON strings
            import json
            return json.dumps(value)
        elif value is None:
             return None # Keep None as SQL NULL
        # Other types (int, float, str, bytes) are handled directly by DB-API
        return value
