#  Bootcamp: Week 2

Complete ETL + EDA pipeline for learning data processing, cleaning, analytics, and statistical inference.

**Note**: This uses synthetically generated data for bootcamp learning.

---

## Quick Start

### 1. Setup
```bash
python -m venv env
source env/Scripts/Activate.ps1  # Windows PowerShell

pip install -r requirements.txt
pip install -e .
```

### 2. Run Pipeline
```bash
python scripts/run_day2_clean.py
python scripts/run_day3_build_analytics.py
```

### 3. Open EDA Notebook
```bash
jupyter notebook notebooks/eda.ipynb
```

---

## What's Inside

**Data Pipeline:**
- Day 1-2: Load raw CSV, clean (duplicates, missing values, validation), export parquet
- Day 3: Build analytics table with joins, time features, outlier handling
- Day 4: EDA with 6 research questions, visualizations, bootstrap confidence intervals

**Main Files:**
- `src/bootcamp_data/` - Reusable modules (config, io, quality, transforms, joins, bootstrap)
- `scripts/` - Day-by-day processing pipelines
- `notebooks/eda.ipynb` - Interactive analysis (start here)
- `reports/` - Exported figures and summary tables
- `pyproject.toml` - All dependencies for reproducibility

---

## Key Findings from EDA

- Revenue: UAE leads with $318.5K (42% of total)
- Trends: Stable monthly revenue around $92-103K, no seasonality
- Orders: Typical amount $200-$300 (mean: $268)
- Refund rates: SA vs AE shows no significant difference (95% CI overlaps zero)
- Data quality: 10% missing amounts (handled), 5% duplicates (removed)

---

## Tech Stack

Python 3.11, pandas, numpy, pyarrow (Parquet), Plotly, Jupyter

---

## Project Structure

```
week 2/
├── data/
│   ├── raw/
│   │   ├── orders.csv          (generated - 5,250 orders)
│   │   └── users.csv           (generated - customer data)
│   ├── processed/
│   │   ├── orders_clean.parquet        (cleaned orders)
│   │   └── analytics_table.parquet     (final analytics table with joins)
│   ├── cache/                  (temporary files)
│   └── external/               (external data sources)
├── src/bootcamp_data/
│   ├── __init__.py
│   ├── config.py               (path management)
│   ├── io.py                   (CSV/Parquet I/O)
│   ├── quality.py              (data validation)
│   ├── transforms.py           (cleaning, deduplication, outliers)
│   ├── joins.py                (safe table joins)
│   └── bootstrap.py            (bootstrap resampling for CI)
├── scripts/
│   ├── main.py                 (project entry point)
│   ├── run_day2_clean.py       (clean raw data)
│   ├── run_day3_build_analytics.py (build analytics table)
│   ├── day4_bootstrap_exercise.py   (bootstrap examples)
│   └── show_analytics_schema.py     (display schema)
├── notebooks/
│   ├── eda.ipynb               (interactive EDA - 6 questions)
│   └── schema_validation.ipynb (schema checks)
├── reports/
│   ├── figures/
│   │   ├── revenue_by_country.png
│   │   ├── revenue_trend_monthly.png
│   │   ├── amount_hist_winsor.png
│   │   ├── amount_by_country_facet.png
│   │   └── revenue_by_country_map.png
│   ├── revenue_by_country.csv  (summary metrics)
│   └── missingness_orders.md   (data quality report)
├── env/                        (virtual environment)
├── bootcamp_data.egg-info/     (package info)
├── pyproject.toml              (dependencies, package config)
├── requirements.txt            (pip requirements)
├── uv.lock                     (uv package lock file)
└── README.md                   (this file)
```

---

## Questions Covered in EDA

1. Which country generates most revenue?
2. How does revenue trend over time?
3. What does a typical order amount look like?
4. Do SA and AE have different refund rates?
5. How do order amounts differ between AE and SA?
6. How are order amounts distributed across countries?

Each question includes code, visualizations, and statistical interpretation.

---

*Complete pipeline: Load → Clean → Validate → Join → Visualize → Analyze*

