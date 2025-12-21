# Bootcamp Data Week 2

A simple data processing project for learning Python, pandas, and parquet files.

## Project Structure

```
week 2/
├── data/
│   ├── raw/              # Raw CSV files (orders, users)
│   ├── processed/        # Cleaned parquet files
│   ├── cache/            # Temporary files
│   └── external/         # External data
├── src/bootcamp_data/    # Main package
│   ├── config.py         # Configuration and paths
│   └── io.py             # Read/write functions
├── scripts/              # Processing scripts
│   ├── create_orders.py
│   ├── process_orders.py
│   └── read_parquet.py
├── notebooks/            # EDA and analysis
│   └── eda.ipynb
└── requirements.txt      # Dependencies
```

## Quick Start

### 1. Setup Environment
```bash
python -m venv env
source env/Scripts/Activate.ps1  # Windows
pip install -r requirements.txt
```

### 2. Install Package
```bash
pip install -e .
```

### 3. Run Scripts
```bash
# Read CSV and convert to parquet
python scripts/create_orders.py

# Clean data and remove NaN
python scripts/process_orders.py

# Read parquet file
python scripts/read_parquet.py
```

## Data Files

- **orders.csv** - Orders data with product info
- **users.csv** - User data with country and signup date
- **order.parquet** - Cleaned orders data (no missing values)

## Requirements

- Python 3.11+
- pandas
- pyarrow
- python-dotenv
