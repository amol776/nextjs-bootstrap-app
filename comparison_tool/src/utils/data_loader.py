"""Utility functions for loading data from various sources."""
import os
import pandas as pd
import zipfile
import sqlalchemy
import requests
from typing import Union, Dict, Any
from pathlib import Path

# Import configurations
LARGE_FILE_THRESHOLD = 3 * 1024 * 1024 * 1024  # 3GB
CHUNK_SIZE = 10 ** 6  # 1 million rows per chunk
TEMP_DIR = "temp"

class DataLoader:
    @staticmethod
    def read_chunked_file(file_path: str, delimiter: str = ',', **kwargs) -> pd.DataFrame:
        """
        Read large files in chunks to handle files > 3GB.
        
        Args:
            file_path: Path to the file
            delimiter: File delimiter (for CSV/DAT files)
            **kwargs: Additional parameters for pd.read_csv
            
        Returns:
            DataFrame containing the file contents
        """
        file_size = os.path.getsize(file_path)
        
        if file_size > LARGE_FILE_THRESHOLD:
            chunks = []
            for chunk in pd.read_csv(file_path, delimiter=delimiter, chunksize=CHUNK_SIZE, **kwargs):
                chunks.append(chunk)
            return pd.concat(chunks, ignore_index=True)
        else:
            return pd.read_csv(file_path, delimiter=delimiter, **kwargs)

    @staticmethod
    def read_parquet(file_path: str) -> pd.DataFrame:
        """Read a parquet file."""
        return pd.read_parquet(file_path)

    @staticmethod
    def extract_zip(zip_path: str) -> list:
        """
        Extract files from a zip archive.
        
        Args:
            zip_path: Path to the zip file
            
        Returns:
            List of paths to extracted files
        """
        extract_dir = Path(TEMP_DIR) / "extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        extracted_files = []
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
            extracted_files = [str(extract_dir / f) for f in zip_ref.namelist()]
        
        return extracted_files

    @staticmethod
    def connect_database(connection_params: Dict[str, Any]) -> sqlalchemy.engine.Engine:
        """
        Create database connection using SQLAlchemy.
        
        Args:
            connection_params: Dictionary containing connection parameters
                Required keys: 'type' (sql_server/teradata), other connection details
                
        Returns:
            SQLAlchemy engine object
        """
        try:
            if connection_params['type'].lower() == 'sql_server':
                conn_str = (
                    f"mssql+pyodbc://{connection_params['username']}:{connection_params['password']}"
                    f"@{connection_params['server']}/{connection_params['database']}"
                    f"?driver=ODBC+Driver+17+for+SQL+Server"
                )
            elif connection_params['type'].lower() == 'teradata':
                conn_str = (
                    f"teradatasql://{connection_params['username']}:{connection_params['password']}"
                    f"@{connection_params['server']}/{connection_params['database']}"
                )
            else:
                raise ValueError(f"Unsupported database type: {connection_params['type']}")

            return sqlalchemy.create_engine(conn_str)
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {connection_params['type']}: {str(e)}")

    @staticmethod
    def execute_stored_proc(engine: sqlalchemy.engine.Engine, proc_name: str, 
                          params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Execute a stored procedure and return results as DataFrame.
        
        Args:
            engine: SQLAlchemy engine object
            proc_name: Name of the stored procedure
            params: Dictionary of procedure parameters
            
        Returns:
            DataFrame containing the procedure results
        """
        try:
            with engine.connect() as conn:
                if params:
                    param_str = ','.join([f"@{k}='{v}'" for k, v in params.items()])
                    query = f"EXEC {proc_name} {param_str}"
                else:
                    query = f"EXEC {proc_name}"
                    
                return pd.read_sql(query, conn)
                
        except Exception as e:
            raise RuntimeError(f"Failed to execute stored procedure {proc_name}: {str(e)}")

    @staticmethod
    def call_api(url: str, method: str = 'GET', headers: Dict[str, str] = None, 
                 params: Dict[str, Any] = None, data: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Call an API endpoint and convert response to DataFrame.
        
        Args:
            url: API endpoint URL
            method: HTTP method (GET/POST)
            headers: Request headers
            params: Query parameters
            data: Request body for POST
            
        Returns:
            DataFrame containing the API response
        """
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data
            )
            response.raise_for_status()
            
            # Assuming JSON response that can be converted to DataFrame
            return pd.DataFrame(response.json())
            
        except Exception as e:
            raise RuntimeError(f"API call failed: {str(e)}")

    @staticmethod
    def cleanup_temp_files():
        """Remove temporary files and directories."""
        if os.path.exists(TEMP_DIR):
            for root, dirs, files in os.walk(TEMP_DIR, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(TEMP_DIR)
