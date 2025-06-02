"""Core comparison engine for data comparison operations."""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
import logging
from ydata_profiling import ProfileReport
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComparisonEngine:
    def __init__(self, source_df: pd.DataFrame, target_df: pd.DataFrame):
        """
        Initialize the comparison engine with source and target dataframes.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
        """
        self.source_df = source_df
        self.target_df = target_df
        self.mapping = None
        self.join_columns = None
        self.excluded_columns = []

    def auto_map_columns(self) -> List[Dict[str, Any]]:
        """
        Automatically map columns between source and target based on names.
        
        Returns:
            List of dictionaries containing column mappings
        """
        source_cols = list(self.source_df.columns)
        target_cols = list(self.target_df.columns)
        mapping = []

        for s_col in source_cols:
            # Try exact match first
            t_col = s_col if s_col in target_cols else None
            
            # If no exact match, try case-insensitive match
            if not t_col:
                t_col = next((col for col in target_cols 
                            if col.lower() == s_col.lower()), None)
            
            # If still no match, try removing special characters
            if not t_col:
                s_clean = ''.join(e.lower() for e in s_col if e.isalnum())
                t_col = next((col for col in target_cols 
                            if ''.join(e.lower() for e in col if e.isalnum()) == s_clean), None)

            mapping.append({
                'source': s_col,
                'target': t_col or '',
                'join': False,
                'data_type': str(self.source_df[s_col].dtype),
                'exclude': False
            })

        return mapping

    def set_mapping(self, mapping: List[Dict[str, Any]], join_columns: List[str]):
        """
        Set the column mapping and join columns for comparison.
        
        Args:
            mapping: List of mapping dictionaries
            join_columns: List of columns to use for joining
        """
        self.mapping = mapping
        self.join_columns = join_columns
        self.excluded_columns = [m['source'] for m in mapping if m['exclude']]

    def _prepare_dataframes(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Prepare dataframes for comparison by applying mappings and type conversions.
        
        Returns:
            Tuple of (prepared source DataFrame, prepared target DataFrame)
        """
        if not self.mapping:
            raise ValueError("Mapping must be set before comparison")

        # Create copies to avoid modifying original dataframes
        source = self.source_df.copy()
        target = self.target_df.copy()

        # Remove excluded columns
        source = source[[m['source'] for m in self.mapping if not m['exclude']]]
        target = target[[m['target'] for m in self.mapping if not m['exclude']]]

        # Rename target columns to match source for comparison
        rename_dict = {m['target']: m['source'] 
                      for m in self.mapping 
                      if not m['exclude'] and m['target']}
        target = target.rename(columns=rename_dict)

        return source, target

    def compare(self) -> Dict[str, Any]:
        """
        Perform the comparison between source and target data.
        
        Returns:
            Dictionary containing comparison results
        """
        source, target = self._prepare_dataframes()
        
        # Initialize comparison results
        results = {
            'match_status': False,
            'rows_match': False,
            'columns_match': False,
            'datacompy_report': '',
            'source_unmatched_rows': pd.DataFrame(),
            'target_unmatched_rows': pd.DataFrame(),
            'column_summary': self._generate_column_summary(source, target),
            'row_counts': {
                'source_name': 'Source',
                'target_name': 'Target',
                'source_count': len(source),
                'target_count': len(target)
            }
        }

        # Basic comparison checks
        results['columns_match'] = set(source.columns) == set(target.columns)
        results['rows_match'] = len(source) == len(target)

        # Detailed comparison
        if self.join_columns:
            # Find unmatched rows
            merged = pd.merge(source, target, on=self.join_columns, how='outer', indicator=True)
            results['source_unmatched_rows'] = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
            results['target_unmatched_rows'] = merged[merged['_merge'] == 'right_only'].drop('_merge', axis=1)
            
            # Generate comparison report
            report_lines = []
            report_lines.append("Comparison Report")
            report_lines.append("-" * 50)
            report_lines.append(f"Source rows: {len(source)}")
            report_lines.append(f"Target rows: {len(target)}")
            report_lines.append(f"Unmatched in source: {len(results['source_unmatched_rows'])}")
            report_lines.append(f"Unmatched in target: {len(results['target_unmatched_rows'])}")
            
            results['datacompy_report'] = "\n".join(report_lines)
            
            # Overall match status
            results['match_status'] = (
                results['columns_match'] and 
                results['rows_match'] and 
                len(results['source_unmatched_rows']) == 0 and
                len(results['target_unmatched_rows']) == 0
            )

        return results

    def _generate_column_summary(self, source: pd.DataFrame, 
                               target: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Generate detailed column-level comparison summary.
        
        Args:
            source: Prepared source DataFrame
            target: Prepared target DataFrame
            
        Returns:
            Dictionary containing column-level statistics
        """
        summary = {}
        
        for col in source.columns:
            if col in self.join_columns:
                continue
                
            summary[col] = {
                'source_null_count': source[col].isnull().sum(),
                'target_null_count': target[col].isnull().sum(),
                'source_unique_count': source[col].nunique(),
                'target_unique_count': target[col].nunique(),
            }
            
            # For numeric columns, add statistical comparisons
            if np.issubdtype(source[col].dtype, np.number):
                summary[col].update({
                    'source_sum': source[col].sum(),
                    'target_sum': target[col].sum(),
                    'source_mean': source[col].mean(),
                    'target_mean': target[col].mean(),
                    'source_std': source[col].std(),
                    'target_std': target[col].std(),
                })

        return summary

    def generate_profiling_reports(self, output_dir: str) -> Dict[str, str]:
        """
        Generate YData Profiling reports for source and target data.
        
        Args:
            output_dir: Directory to save the reports
            
        Returns:
            Dictionary containing paths to generated reports
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Generate individual profiles
        source_profile = ProfileReport(self.source_df, title="Source Data Profile")
        target_profile = ProfileReport(self.target_df, title="Target Data Profile")

        # Save reports
        source_path = output_path / "source_profile.html"
        target_path = output_path / "target_profile.html"
        comparison_path = output_path / "comparison_profile.html"

        source_profile.to_file(str(source_path))
        target_profile.to_file(str(target_path))
        
        # Generate comparison report
        comparison_report = source_profile.compare(target_profile)
        comparison_report.to_file(str(comparison_path))

        return {
            'source_profile': str(source_path),
            'target_profile': str(target_path),
            'comparison_profile': str(comparison_path)
        }

    def get_distinct_values(self, columns: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get distinct values and their counts for specified columns.
        
        Args:
            columns: List of columns to analyze. If None, analyze all non-numeric columns.
            
        Returns:
            Dictionary containing distinct values and counts for each column
        """
        source, target = self._prepare_dataframes()
        
        if not columns:
            columns = [col for col in source.columns 
                      if not np.issubdtype(source[col].dtype, np.number)]

        distinct_values = {}
        for col in columns:
            if col in source.columns and col in target.columns:
                source_distinct = source[col].value_counts().to_dict()
                target_distinct = target[col].value_counts().to_dict()
                
                distinct_values[col] = {
                    'source_values': source_distinct,
                    'target_values': target_distinct,
                    'source_count': len(source_distinct),
                    'target_count': len(target_distinct),
                    'matching': set(source_distinct.keys()) == set(target_distinct.keys())
                }

        return distinct_values
