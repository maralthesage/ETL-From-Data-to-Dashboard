# ETL Pipeline for Customer Data Analytics

A comprehensive ETL (Extract, Transform, Load) pipeline for processing customer data and performing advanced analytics including RFM (Recency, Frequency, Monetary) analysis for customer segmentation.

**Copyright (c) 2025 ETL Pipeline Project**  
**Licensed under the MIT License - see [LICENSE](LICENSE) file for details.**

## 🚀 Features

- **Multi-source Data Integration**: Processes customer data from multiple sources (addresses, transactions, emails, advertising)
- **RFM Customer Segmentation**: Advanced customer segmentation using RFM analysis
- **Prefect Orchestration**: Workflow orchestration with Prefect for reliable data processing
- **Scalable Processing**: Support for both pandas and Dask for handling large datasets
- **Data Quality Validation**: Built-in data quality checks and validation
- **Configurable Pipeline**: Easy configuration for different countries and data sources
- **Comprehensive Logging**: Detailed logging and monitoring capabilities

## 📁 Project Structure

```
ETL-From-Data-to-Dashboard/
├── src/
│   ├── etl/
│   │   └── customer_pipeline.py      # Main customer data ETL pipeline
│   ├── analytics/
│   │   └── rfm_analysis.py          # RFM analysis and customer segmentation
│   ├── utils/
│   │   └── data_processing.py       # Utility functions for data processing
│   └── main.py                      # Main orchestration script
├── config/
│   └── settings.py                  # Configuration settings
├── data/
│   ├── raw/                         # Raw data files
│   ├── processed/                   # Processed data files
│   └── output/                      # Final output files
├── docs/                            # Documentation
├── tests/                           # Test files
├── requirements.txt                 # Python dependencies
├── .gitignore                       # Git ignore rules
└── README.md                        # This file
```

## 🛠️ Installation

### Prerequisites

- Python 3.8 or higher
- pip or conda package manager

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ETL-From-Data-to-Dashboard
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   export DATA_PATH="/path/to/your/data"
   export OUTPUT_PATH="/path/to/your/output"
   ```

## 📊 Data Sources

The pipeline processes the following data sources:

### Customer Data
- **Addresses**: Customer demographic information, registration dates
- **Transactions**: Sales transactions, order details, financial data
- **Emails**: Email preferences and communication settings
- **Advertising**: Marketing campaign data and customer touchpoints

### Expected File Formats
- CSV files with semicolon (`;`) separator
- CP850 encoding
- Specific column naming conventions (see configuration)

## 🚀 Usage

### Basic Usage

Run the complete ETL pipeline for all supported countries:

```bash
python src/main.py --mode complete
```

### Advanced Usage

#### Run specific pipeline components

```bash
# Customer data processing only
python src/main.py --mode customer --countries F01 F02

# RFM analysis only  
python src/main.py --mode rfm --countries F01

# Complete pipeline for specific countries
python src/main.py --mode complete --countries F01 F02 F03
```

#### Custom data paths

```bash
python src/main.py --mode complete --data-path /custom/data/path --output-path /custom/output/path
```

### Programmatic Usage

```python
from src.main import run_complete_etl_pipeline

# Run complete pipeline
results = run_complete_etl_pipeline(
    countries=['F01', 'F02'],
    data_path='/path/to/data',
    output_path='/path/to/output'
)
```

## ⚙️ Configuration

### Environment Variables

Set the following environment variables:

```bash
DATA_PATH="/path/to/your/source/data"
OUTPUT_PATH="/path/to/your/output/directory"
```

### Configuration File

Modify `config/settings.py` to customize:

- **Country mappings**: Add new countries or modify existing ones
- **File paths**: Update data source file names
- **RFM parameters**: Adjust segmentation thresholds
- **Processing settings**: Modify data processing parameters

### Data Source Configuration

The pipeline expects data files in the following structure:

```
data/
├── F01/  # Germany
│   ├── customer_addresses.csv
│   ├── customer_transactions.csv
│   ├── customer_emails.csv
│   └── advertising_data.csv
├── F02/  # France
│   └── ...
└── ...
```

## 📈 Pipeline Components

### 1. Customer Data ETL Pipeline

**Purpose**: Extract, transform, and load customer data from multiple sources

**Key Features**:
- Data quality validation
- Customer ID standardization
- Age group calculation
- Source attribution
- Data merging and aggregation

**Output**: Clean, aggregated customer dataset ready for analysis

### 2. RFM Analysis Pipeline

**Purpose**: Perform RFM (Recency, Frequency, Monetary) analysis for customer segmentation

**Key Features**:
- Time-based metric calculation
- RFM score computation
- Customer segmentation
- Segment distribution analysis

**Output**: Customer segments with RFM scores and segment labels

### 3. Data Processing Utilities

**Purpose**: Reusable functions for data cleaning and transformation

**Key Features**:
- Customer ID padding and standardization
- Age group calculation
- Source attribution
- Data validation
- String processing utilities

## 🔧 Prefect Orchestration

The pipeline uses Prefect for workflow orchestration, providing:

- **Task Dependencies**: Automatic task dependency management
- **Error Handling**: Robust error handling and retry mechanisms
- **Monitoring**: Built-in monitoring and logging
- **Scalability**: Easy scaling across multiple workers
- **Scheduling**: Built-in scheduling capabilities

### Prefect Dashboard

Access the Prefect dashboard at `http://localhost:4200` to monitor pipeline execution.

## 📊 Output Files

The pipeline generates the following output files:

### Customer Analysis
- `customer_analysis_{country}.csv`: Aggregated customer data
- Contains: Customer demographics, transaction history, source attribution

### RFM Segments  
- `rfm_segments_{country}.csv`: Customer segmentation results
- Contains: RFM scores, customer segments, segment labels

### Customer Groups
- `customer_groups_{country}.csv`: Customer group classifications
- Contains: Customer group assignments and classifications

## 🧪 Testing

Run the test suite:

```bash
pytest tests/ -v
```

Run with coverage:

```bash
pytest tests/ --cov=src --cov-report=html
```

## 📝 Logging

The pipeline provides comprehensive logging:

- **Task-level logging**: Individual task execution details
- **Data quality metrics**: Validation results and data statistics
- **Performance metrics**: Processing times and resource usage
- **Error tracking**: Detailed error messages and stack traces

## 🔒 Data Privacy

This pipeline is designed with data privacy in mind:

- **No personal data**: All personal identifiers are anonymized
- **Configurable paths**: Easy to point to different data sources
- **Git ignore**: Sensitive data files are excluded from version control
- **Environment variables**: Sensitive paths configured via environment variables

## 🚀 Deployment

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export DATA_PATH="/path/to/data"
export OUTPUT_PATH="/path/to/output"

# Run pipeline
python src/main.py --mode complete
```

### Production Deployment

1. **Set up Prefect server**:
   ```bash
   prefect server start
   ```

2. **Deploy flows**:
   ```bash
   prefect deployment build src/main.py:run_complete_etl_pipeline -n "customer-etl"
   prefect deployment apply run_complete_etl_pipeline-deployment.yaml
   ```

3. **Schedule execution**:
   ```bash
   prefect deployment run "customer-etl/run_complete_etl_pipeline"
   ```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For questions and support:

- Create an issue in the repository
- Contact the development team
- Check the documentation in the `docs/` folder

## 🔄 Version History

- **v1.0.0**: Initial release with basic ETL pipeline
- **v1.1.0**: Added RFM analysis capabilities
- **v1.2.0**: Enhanced data processing utilities
- **v1.3.0**: Added Prefect orchestration
- **v2.0.0**: Complete refactoring with modular architecture

## 🎯 Future Enhancements

- [ ] Real-time data processing capabilities
- [ ] Advanced machine learning models for customer prediction
- [ ] Interactive dashboards for data visualization
- [ ] Cloud deployment configurations
- [ ] Additional data source connectors
- [ ] Enhanced data quality monitoring
