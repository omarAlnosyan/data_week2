# Data Engineering Bootcamp: Week 2

**Complete ETL + EDA pipeline** combining data cleaning, analytics, visualization, and statistical inference across 4 days.

---

## Project Structure

```
week 2/
├── data/
│   ├── raw/                      # Raw CSV files (orders, users)
│   ├── processed/                # Cleaned parquet outputs
│   ├── cache/                    # Temporary files
│   └── external/                 # External data
├── src/bootcamp_data/            # Core processing package
│   ├── config.py                 # Path management (Paths dataclass, make_paths)
│   ├── io.py                     # CSV/Parquet I/O with custom NA handling
│   ├── quality.py                # Assertion-based validation (require_columns, assert_unique_key, etc.)
│   ├── transforms.py             # Schema enforcement, cleaning, datetime/outlier helpers, deduplication
│   ├── joins.py                  # Safe left join with validation
│   └── bootstrap.py              # Bootstrap resampling for 95% CIs
├── scripts/                      # Daily processing pipelines
│   ├── run_day1_load.py          # Initial data loading
│   ├── run_day2_clean.py         # Cleaning + quality checks + missingness report
│   ├── run_day3_build_analytics.py # Analytics table with joins and time features
│   ├── day4_bootstrap_exercise.py  # Bootstrap SA vs AE refund rate comparison
│   └── show_analytics_schema.py   # Display schema of analytics_table.parquet
├── notebooks/
│   └── eda.ipynb                 # Complete EDA with 5 questions, 5 figures, 2 bootstrap CIs
├── reports/
│   ├── figures/                  # Exported PNG visualizations
│   │   ├── revenue_by_country.png
│   │   ├── revenue_trend_monthly.png
│   │   ├── amount_hist_winsor.png
│   │   ├── amount_by_country_facet.png
│   │   └── revenue_by_country_map.png
│   └── revenue_by_country.csv    # Summary table (AE: $318.5K, QA: $299K, KW: $290K, SA: $280K)
├── pyproject.toml                # Package configuration
├── requirements.txt              # Dependencies
└── README.md                     # This file
```


---

## Quick Start

### 1. Setup Environment
```bash
# Create and activate virtual environment
python -m venv env
source env/Scripts/Activate.ps1  # Windows PowerShell
# or: env\Scripts\activate.bat   # Windows cmd

# Install dependencies
pip install -r requirements.txt

# Install package in editable mode
pip install -e .
```

### 3. Running the EDA Notebook

#### Option A: Using Jupyter (Interactive)
```bash
# Start Jupyter Lab/Notebook
jupyter notebook notebooks/eda.ipynb

# Or use JupyterLab for modern interface
jupyter lab notebooks/eda.ipynb
```

#### Option B: Using UV (Recommended - Modern Python Package Manager)
```bash
# Sync dependencies from pyproject.toml
uv sync

# Run the notebook
uv run jupyter notebook notebooks/eda.ipynb
```

#### Option C: Using Virtual Environment (Traditional)
```bash
# Activate your venv first
source env/Scripts/Activate.ps1  # Windows PowerShell
# or: env\Scripts\activate.bat   # Windows cmd

# Then run Jupyter
jupyter notebook notebooks/eda.ipynb
```

---

## Technology Stack

- **Language**: Python 3.11
- **Data Processing**: pandas, numpy
- **Storage**: Apache Arrow (pyarrow, parquet)
- **Visualization**: Plotly Express + kaleido (PNG export)
- **Statistical Inference**: NumPy random sampling (bootstrap)
- **Version Control**: Git + GitHub

---

## Requirements

```
pandas>=2.0
numpy>=1.24
pyarrow>=12.0
plotly>=5.0
kaleido>=0.2.1
python-dotenv>=0.21
```

