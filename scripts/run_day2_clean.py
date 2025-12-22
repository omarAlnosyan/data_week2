"""
Day 2: Data Cleaning Pipeline
Cleans orders data and generates quality reports
"""

from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bootcamp_data.io import read_orders_csv, write_parquet, read_parquet
from bootcamp_data.config import make_paths
from bootcamp_data.transforms import clean_orders
from bootcamp_data.quality import quality_report, check_missing_values


def main():
    """Run the complete cleaning pipeline"""
    
    # Setup
    root = Path(__file__).parent.parent
    paths = make_paths(root)
    
    print("=" * 60)
    print("DATA CLEANING PIPELINE - DAY 2")
    print("=" * 60)
    
    # Step 1: Read raw data
    print("\n1. Reading raw orders data...")
    raw_csv = paths.raw / "orders.csv"
    df_raw = read_orders_csv(raw_csv)
    print(f"   ✓ Loaded {len(df_raw)} rows")
    
    # Step 2: Generate before report
    print("\n2. Quality analysis BEFORE cleaning...")
    before_report = quality_report(df_raw)
    print(f"   Total rows: {before_report['total_rows']}")
    print(f"   Total columns: {before_report['total_columns']}")
    print(f"   Duplicate rows: {before_report['duplicate_rows']}")
    print(f"   Missing values:")
    for col, count in before_report['missing_values'].items():
        if count > 0:
            print(f"     - {col}: {count}")
    
    # Step 3: Clean data
    print("\n3. Cleaning data...")
    df_clean = clean_orders(df_raw)
    print(f"   ✓ Rows after cleaning: {len(df_clean)}")
    print(f"   ✓ Rows removed: {len(df_raw) - len(df_clean)}")
    
    # Step 4: Generate after report
    print("\n4. Quality analysis AFTER cleaning...")
    after_report = quality_report(df_clean)
    print(f"   Total rows: {after_report['total_rows']}")
    print(f"   Missing values:")
    for col, count in after_report['missing_values'].items():
        if count > 0:
            print(f"     - {col}: {count}")
    
    # Step 5: Save cleaned data
    print("\n5. Saving cleaned data...")
    output_path = paths.processed / "orders_clean.parquet"
    write_parquet(df_clean, output_path)
    print(f"   ✓ Saved to: {output_path}")
    
    # Step 6: Generate missingness report
    print("\n6. Generating missingness report...")
    missingness_report = create_missingness_report(df_raw, df_clean)
    report_path = paths.root / "reports" / "missingness_orders.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, 'w') as f:
        f.write(missingness_report)
    print(f"   ✓ Saved to: {report_path}")
    
    # Step 7: Verify cleaned data
    print("\n7. Verifying cleaned data...")
    df_verify = read_parquet(output_path)
    print(f"   ✓ Verified: {len(df_verify)} rows")
    print(f"   ✓ Columns: {list(df_verify.columns)}")
    
    print("\n" + "=" * 60)
    print("CLEANING PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\nOutputs:")
    print(f"  - Cleaned data: {output_path}")
    print(f"  - Quality report: {report_path}")


def create_missingness_report(df_before, df_after) -> str:
    """
    Create a markdown report of data missingness
    
    Args:
        df_before: Raw dataframe
        df_after: Cleaned dataframe
        
    Returns:
        Markdown formatted report
    """
    
    missing_before = check_missing_values(df_before)
    missing_after = check_missing_values(df_after)
    
    report = "# Data Missingness Report\n\n"
    report += f"**Report Date:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    report += "## Summary\n"
    report += f"- **Rows before cleaning:** {len(df_before)}\n"
    report += f"- **Rows after cleaning:** {len(df_after)}\n"
    report += f"- **Rows removed:** {len(df_before) - len(df_after)}\n\n"
    
    report += "## Missing Values Analysis\n\n"
    report += "| Column | Before | After | Improvement |\n"
    report += "|--------|--------|-------|-------------|\n"
    
    for col in df_before.columns:
        before = missing_before.get(col, 0)
        after = missing_after.get(col, 0)
        improvement = before - after
        report += f"| {col} | {before} | {after} | {improvement} |\n"
    
    report += "\n## Data Quality Improvements\n\n"
    
    improvements = []
    for col in df_before.columns:
        before = missing_before.get(col, 0)
        after = missing_after.get(col, 0)
        if before > after:
            pct = ((before - after) / before * 100) if before > 0 else 0
            improvements.append(f"- **{col}**: Reduced missing values from {before} to {after} ({pct:.1f}% improvement)")
    
    if improvements:
        for imp in improvements:
            report += imp + "\n"
    else:
        report += "- No missing values to clean\n"
    
    return report


if __name__ == "__main__":
    main()
