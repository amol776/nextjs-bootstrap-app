"""Main Streamlit application for the Comparison Tool."""
import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import os
from typing import Dict, Any, Tuple

# Import local modules
from utils.data_loader import DataLoader
from utils.comparison_engine import ComparisonEngine
from reports.report_generator import ReportGenerator

# Constants
SUPPORTED_SOURCES = [
    'CSV file',
    'DAT file',
    'SQL Server',
    'Stored Procs',
    'Teradata',
    'API',
    'Parquet file',
    'Flat files inside zipped folder'
]

TYPE_MAPPING = {
    'int': 'int32',
    'int64': 'int64',
    'numeric': 'int64',
    'bigint': 'int64',
    'smallint': 'int64',
    'varchar': 'string',
    'nvarchar': 'string',
    'char': 'string',
    'date': 'datetime64[ns]',
    'datetime': 'datetime64[ns]',
    'decimal': 'float',
    'float': 'float',
    'bit': 'bool',
    'nchar': 'char',
    'boolean': 'bool'
}

MAX_PREVIEW_ROWS = 1000

# Page configuration
st.set_page_config(
    page_title="Data Comparison Tool",
    page_icon="📊",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main {
        padding: 2rem;
    }
    .stButton > button {
        width: 100%;
    }
    .report-box {
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e0e0e0;
        margin: 1rem 0;
    }
    .header-banner {
        padding: 2rem;
        background: linear-gradient(90deg, #1E3D59 0%, #1E3D59 100%);
        color: white;
        border-radius: 0.5rem;
        margin-bottom: 2rem;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# Session state initialization
if 'comparison_results' not in st.session_state:
    st.session_state.comparison_results = None
if 'report_paths' not in st.session_state:
    st.session_state.report_paths = None

def load_data(source_type: str, file_upload, connection_params: Dict[str, Any] = None, 
              delimiter: str = ',') -> pd.DataFrame:
    """Load data based on the selected source type."""
    loader = DataLoader()
    
    if source_type in ['CSV file', 'DAT file']:
        if file_upload is None:
            raise ValueError(f"Please upload a {source_type}")
            
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_upload.name) as tmp_file:
            tmp_file.write(file_upload.getvalue())
            return loader.read_chunked_file(tmp_file.name, delimiter=delimiter)
            
    elif source_type == 'Parquet file':
        if file_upload is None:
            raise ValueError("Please upload a Parquet file")
            
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
            tmp_file.write(file_upload.getvalue())
            return loader.read_parquet(tmp_file.name)
            
    elif source_type == 'Flat files inside zipped folder':
        if file_upload is None:
            raise ValueError("Please upload a ZIP file")
            
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp_file:
            tmp_file.write(file_upload.getvalue())
            extracted_files = loader.extract_zip(tmp_file.name)
            
            # Combine all extracted files
            dfs = []
            for file_path in extracted_files:
                if file_path.endswith(('.csv', '.dat')):
                    df = loader.read_chunked_file(file_path, delimiter=delimiter)
                    dfs.append(df)
            
            if not dfs:
                raise ValueError("No valid files found in ZIP archive")
            return pd.concat(dfs, ignore_index=True)
            
    elif source_type in ['SQL Server', 'Teradata']:
        if not connection_params:
            raise ValueError(f"Please provide connection parameters for {source_type}")
            
        engine = loader.connect_database(connection_params)
        if 'query' in connection_params:
            return pd.read_sql(connection_params['query'], engine)
        else:
            return pd.read_sql(connection_params['table'], engine)
            
    elif source_type == 'Stored Procs':
        if not connection_params:
            raise ValueError("Please provide stored procedure details")
            
        engine = loader.connect_database(connection_params)
        return loader.execute_stored_proc(
            engine,
            connection_params['proc_name'],
            connection_params.get('params')
        )
        
    elif source_type == 'API':
        if not connection_params:
            raise ValueError("Please provide API details")
            
        return loader.call_api(
            connection_params['url'],
            method=connection_params.get('method', 'GET'),
            headers=connection_params.get('headers'),
            params=connection_params.get('params'),
            data=connection_params.get('data')
        )
        
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

def get_connection_inputs(source_type: str) -> Dict[str, Any]:
    """Get connection parameters based on source type."""
    params = {}
    
    if source_type in ['SQL Server', 'Teradata']:
        col1, col2 = st.columns(2)
        with col1:
            params['server'] = st.text_input(f"{source_type} Server")
            params['database'] = st.text_input("Database Name")
        with col2:
            params['username'] = st.text_input("Username")
            params['password'] = st.text_input("Password", type="password")
        params['type'] = source_type
        
        # Option to use query or table
        query_type = st.radio("Select Input Type", ["Table", "Query"])
        if query_type == "Table":
            params['table'] = st.text_input("Table Name")
        else:
            params['query'] = st.text_area("SQL Query")
            
    elif source_type == 'Stored Procs':
        col1, col2 = st.columns(2)
        with col1:
            params['server'] = st.text_input("Database Server")
            params['database'] = st.text_input("Database Name")
        with col2:
            params['username'] = st.text_input("Username")
            params['password'] = st.text_input("Password", type="password")
        
        params['type'] = 'SQL Server'  # Default to SQL Server for stored procs
        params['proc_name'] = st.text_input("Stored Procedure Name")
        
        # Optional procedure parameters
        if st.checkbox("Add Procedure Parameters"):
            param_count = st.number_input("Number of Parameters", min_value=1, value=1)
            params['params'] = {}
            for i in range(param_count):
                col1, col2 = st.columns(2)
                with col1:
                    param_name = st.text_input(f"Parameter {i+1} Name")
                with col2:
                    param_value = st.text_input(f"Parameter {i+1} Value")
                if param_name:
                    params['params'][param_name] = param_value
                    
    elif source_type == 'API':
        params['url'] = st.text_input("API URL")
        params['method'] = st.selectbox("HTTP Method", ["GET", "POST"])
        
        if st.checkbox("Add Headers"):
            params['headers'] = {}
            header_count = st.number_input("Number of Headers", min_value=1, value=1)
            for i in range(header_count):
                col1, col2 = st.columns(2)
                with col1:
                    header_name = st.text_input(f"Header {i+1} Name")
                with col2:
                    header_value = st.text_input(f"Header {i+1} Value")
                if header_name:
                    params['headers'][header_name] = header_value
        
        if params['method'] == "POST":
            params['data'] = st.text_area("Request Body (JSON)")
            
    return params

def main():
    # Header
    st.markdown("""
        <div class="header-banner">
            <h1>Data Comparison Tool</h1>
            <p>Compare data across multiple sources with detailed analysis and reporting</p>
        </div>
    """, unsafe_allow_html=True)

    # Source Selection
    st.subheader("1. Select Source and Target")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Source")
        source_type = st.selectbox("Select Source Type", SUPPORTED_SOURCES, key="source_type")
        
        # File upload or connection parameters for source
        source_data = None
        if source_type in ['CSV file', 'DAT file', 'Parquet file', 'Flat files inside zipped folder']:
            source_file = st.file_uploader(f"Upload {source_type}", key="source_file")
            if source_type in ['CSV file', 'DAT file', 'Flat files inside zipped folder']:
                source_delimiter = st.text_input("Source Delimiter", ",", key="source_delimiter")
        else:
            source_params = get_connection_inputs(source_type)
            
    with col2:
        st.markdown("### Target")
        target_type = st.selectbox("Select Target Type", SUPPORTED_SOURCES, key="target_type")
        
        # File upload or connection parameters for target
        target_data = None
        if target_type in ['CSV file', 'DAT file', 'Parquet file', 'Flat files inside zipped folder']:
            target_file = st.file_uploader(f"Upload {target_type}", key="target_file")
            if target_type in ['CSV file', 'DAT file', 'Flat files inside zipped folder']:
                target_delimiter = st.text_input("Target Delimiter", ",", key="target_delimiter")
        else:
            target_params = get_connection_inputs(target_type)

    # Load Data button
    if st.button("Load Data"):
        try:
            with st.spinner("Loading source data..."):
                if source_type in ['CSV file', 'DAT file', 'Parquet file', 'Flat files inside zipped folder']:
                    source_data = load_data(source_type, st.session_state.source_file,
                                         delimiter=st.session_state.get('source_delimiter', ','))
                else:
                    source_data = load_data(source_type, None, source_params)
                    
            with st.spinner("Loading target data..."):
                if target_type in ['CSV file', 'DAT file', 'Parquet file', 'Flat files inside zipped folder']:
                    target_data = load_data(target_type, st.session_state.target_file,
                                         delimiter=st.session_state.get('target_delimiter', ','))
                else:
                    target_data = load_data(target_type, None, target_params)
                    
            # Store in session state
            st.session_state.source_data = source_data
            st.session_state.target_data = target_data
            
            # Show preview
            st.subheader("Data Preview")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("### Source Data")
                st.dataframe(source_data.head(MAX_PREVIEW_ROWS))
            with col2:
                st.markdown("### Target Data")
                st.dataframe(target_data.head(MAX_PREVIEW_ROWS))
                
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            return

    # Column Mapping
    if 'source_data' in st.session_state and 'target_data' in st.session_state:
        st.subheader("2. Column Mapping")
        
        # Initialize comparison engine
        engine = ComparisonEngine(st.session_state.source_data, st.session_state.target_data)
        
        # Get automatic mapping
        if 'column_mapping' not in st.session_state:
            st.session_state.column_mapping = engine.auto_map_columns()
        
        # Create mapping editor
        mapping_data = pd.DataFrame(st.session_state.column_mapping)
        edited_mapping = st.data_editor(
            mapping_data,
            column_config={
                "source": st.column_config.TextColumn("Source Column", disabled=True),
                "target": st.column_config.SelectboxColumn(
                    "Target Column",
                    options=[""] + list(st.session_state.target_data.columns)
                ),
                "join": st.column_config.CheckboxColumn("Join Column"),
                "data_type": st.column_config.SelectboxColumn(
                    "Data Type",
                    options=list(TYPE_MAPPING.values())
                ),
                "exclude": st.column_config.CheckboxColumn("Exclude")
            },
            hide_index=True
        )
        
        # Update session state
        st.session_state.column_mapping = edited_mapping.to_dict('records')
        
        # Get selected join columns
        join_columns = [m['source'] for m in st.session_state.column_mapping if m['join']]
        
        if not join_columns:
            st.warning("Please select at least one join column")
        else:
            # Compare button
            if st.button("Compare"):
                try:
                    with st.spinner("Performing comparison..."):
                        # Set mapping in comparison engine
                        engine.set_mapping(st.session_state.column_mapping, join_columns)
                        
                        # Perform comparison
                        comparison_results = engine.compare()
                        
                        # Generate reports
                        report_gen = ReportGenerator("reports")
                        
                        # Generate and save reports
                        report_paths = {}
                        
                        # Regression report
                        report_paths['regression'] = report_gen.generate_regression_report(comparison_results)
                        
                        # Difference report
                        if 'source_unmatched_rows' in comparison_results:
                            differences = pd.concat([
                                comparison_results['source_unmatched_rows'],
                                comparison_results['target_unmatched_rows']
                            ])
                            report_paths['differences'] = report_gen.generate_difference_report(differences)
                        
                        # Profile reports
                        profile_paths = engine.generate_profiling_reports("reports")
                        report_paths.update(profile_paths)
                        
                        # Create ZIP archive
                        zip_path = report_gen.create_report_archive(report_paths)
                        
                        # Store results in session state
                        st.session_state.comparison_results = comparison_results
                        st.session_state.report_paths = report_paths
                        st.session_state.zip_path = zip_path
                        
                except Exception as e:
                    st.error(f"Comparison failed: {str(e)}")
                    return

    # Display Results
    if st.session_state.comparison_results:
        st.subheader("3. Comparison Results")
        
        # Summary statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Rows Match", "Yes" if st.session_state.comparison_results['rows_match'] else "No")
        with col2:
            st.metric("Columns Match", "Yes" if st.session_state.comparison_results['columns_match'] else "No")
        with col3:
            st.metric("Overall Match", "Yes" if st.session_state.comparison_results['match_status'] else "No")
        
        # DataCompy Report
        with st.expander("View DataCompy Report"):
            st.text(st.session_state.comparison_results['datacompy_report'])
        
        # Download buttons
        st.subheader("4. Download Reports")
        
        col1, col2 = st.columns(2)
        with col1:
            # Individual report downloads
            for report_type, path in st.session_state.report_paths.items():
                with open(path, 'rb') as f:
                    st.download_button(
                        f"Download {report_type.replace('_', ' ').title()} Report",
                        f,
                        file_name=os.path.basename(path),
                        mime='application/octet-stream'
                    )
        
        with col2:
            # ZIP archive download
            with open(st.session_state.zip_path, 'rb') as f:
                st.download_button(
                    "Download All Reports (ZIP)",
                    f,
                    file_name=os.path.basename(st.session_state.zip_path),
                    mime='application/zip'
                )

if __name__ == "__main__":
    main()
