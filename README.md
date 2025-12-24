# Data Engineering Bootcamp: Week 2

**Complete ETL + EDA pipeline** combining data cleaning, analytics, visualization, and statistical inference across 4 days.

- **Days 1-2**: Data loading, quality checks, and comprehensive cleaning  
- **Day 3**: Analytics table construction with datetime/outlier handling and safe joins  
- **Day 4**: Exploratory data analysis with Plotly, bootstrap confidence intervals, and geographic visualizations  

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

## Overview

**Days 1-2**: Clean raw CSVs → Remove duplicates, standardize status, flag missing values → `orders_clean.parquet`

**Day 3**: Build analytics table → Parse datetimes, extract time features, detect outliers, safe join users → `analytics_table.parquet` (17 cols, 5,250 rows)

**Day 4**: EDA with 5 questions → 5 exported charts, 2 bootstrap CIs, data dictionary

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

### 2. Run Day-by-Day Scripts
```bash
# Day 1-2: Load raw data and clean
python scripts/run_day2_clean.py

# Day 3: Build analytics table with joins
python scripts/run_day3_build_analytics.py

# Day 4: Run EDA notebook (or open in Jupyter)
jupyter notebook notebooks/eda.ipynb
```

### 3. Key Outputs
- **Cleaned orders**: `data/processed/orders_clean.parquet`
- **Analytics table**: `data/processed/analytics_table.parquet`
- **Revenue summary**: `reports/revenue_by_country.csv`
- **5 Figures**: `reports/figures/*.png`

---

## Technology Stack

- **Language**: Python 3.11
- **Data Processing**: pandas, numpy
- **Storage**: Apache Arrow (pyarrow, parquet)
- **Visualization**: Plotly Express + kaleido (PNG export)
- **Statistical Inference**: NumPy random sampling (bootstrap)
- **Version Control**: Git + GitHub
- **Documentation**: Markdown

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

---

## Notes

- **Data Quality**: 10% missing amounts (handled with NaN flags), 5% duplicates (removed), mock data generation for learning
- **Bootstrap Interpretation**: CI overlapping zero means inconclusive difference (not significantly different)
- **Timezone**: All datetimes parsed to UTC
- **Outliers**: Flagged via IQR method (k=1.5), clipped via 1st–99th percentile winsorization for viz

---

*Complete bootcamp pipeline: Data Cleaning → Analytics Table → EDA → Statistical Inference*
