# Data Comparison Tool

A powerful and flexible tool for comparing data across multiple sources with detailed analysis and reporting capabilities.

## Features

- **Multiple Data Source Support**:
  - CSV files
  - DAT files
  - SQL Server databases
  - Stored Procedures
  - Teradata
  - REST APIs
  - Parquet files
  - Zipped flat files

- **Advanced Comparison Capabilities**:
  - Column mapping with automatic detection
  - Data type validation and conversion
  - Join key selection
  - Row-level comparison
  - Aggregation checks
  - Distinct value analysis

- **Comprehensive Reporting**:
  - Excel-based regression reports
  - Side-by-side difference reports
  - YData profiling reports
  - Detailed statistical analysis
  - Downloadable ZIP archives of all reports

- **Performance Features**:
  - Chunked processing for large files
  - Memory-efficient operations
  - Progress tracking
  - Configurable thresholds

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/comparison-tool.git
cd comparison-tool
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Start the application:
```bash
cd src
streamlit run app.py
```

2. Access the tool through your web browser at `http://localhost:8000`

3. Select source and target data:
   - Choose data source types
   - Upload files or provide connection details
   - Configure delimiters for file-based sources

4. Map columns:
   - Review automatic column mapping
   - Adjust mappings manually if needed
   - Select join columns
   - Configure data type conversions
   - Exclude columns if desired

5. Generate reports:
   - Click "Compare" to start analysis
   - View comparison results
   - Download individual reports or complete ZIP archive

## Sample Data

The `src/sample_data` directory contains example files for testing:

- `source.csv`: Sample source data
- `target.csv`: Sample target data with intentional differences

## Configuration

Key settings can be adjusted in `src/config.py`:

- File processing thresholds
- Database connection timeouts
- Report generation options
- Data type mappings
- Error thresholds

## Project Structure

```
comparison_tool/
├── README.md
├── requirements.txt
└── src/
    ├── app.py                 # Main Streamlit application
    ├── config.py             # Configuration settings
    ├── utils/
    │   ├── data_loader.py    # Data source handlers
    │   └── comparison_engine.py  # Core comparison logic
    ├── reports/
    │   └── report_generator.py   # Report generation
    └── sample_data/          # Example files
        ├── source.csv
        └── target.csv
```

## Dependencies

- streamlit
- pandas
- numpy
- openpyxl
- ydata-profiling
- sqlalchemy
- pyodbc
- teradatasql
- requests

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, please open an issue in the GitHub repository or contact the development team.
