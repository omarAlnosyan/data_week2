"""
Day 3: Build Analytics Table
Loads cleaned orders and users, applies datetime and outlier transformations,
joins tables, and generates comprehensive analytics table.
"""
import logging
from pathlib import Path

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_parquet, write_parquet
from bootcamp_data.transforms import (
    parse_datetime,
    add_time_parts,
    add_outlier_flag,
    iqr_bounds,
)
from bootcamp_data.joins import safe_left_join
from bootcamp_data.quality import assert_non_empty

log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    p = make_paths(ROOT)

    # 1. Load processed data
    log.info("Loading processed data")
    orders = read_parquet(p.processed / "orders_clean.parquet")
    users = read_parquet(p.processed / "users.parquet")
    log.info("Rows: orders=%s, users=%s", len(orders), len(users))

    # 2. Parse datetime columns
    log.info("Parsing datetime columns")
    orders = parse_datetime(orders, "created_at", utc=True)
    users = parse_datetime(users, "signup_date", utc=True)

    # 3. Add time parts to orders
    log.info("Extracting time features from created_at")
    orders = add_time_parts(orders, "created_at")

    # 4. Add outlier flags for amount
    log.info("Computing outlier bounds for amount")
    lo, hi = iqr_bounds(orders["amount"], k=1.5)
    log.info("Amount outlier bounds: [%.2f, %.2f]", lo, hi)
    orders = add_outlier_flag(orders, "amount", k=1.5)

    # 5. Join orders with users
    log.info("Joining orders with users")
    analytics = safe_left_join(
        orders,
        users,
        on="user_id",
        how="left",
        validate="m:1",
    )
    assert_non_empty(analytics, "analytics table")

    # 6. Create summary statistics
    log.info("Computing summary statistics")
    summary = {
        "total_orders": len(analytics),
        "total_users": analytics["user_id"].nunique(),
        "total_amount": analytics["amount"].sum(),
        "avg_amount": analytics["amount"].mean(),
        "median_amount": analytics["amount"].median(),
        "outliers_count": analytics["amount__is_outlier"].sum(),
        "outliers_pct": (analytics["amount__is_outlier"].sum() / len(analytics)) * 100,
        "null_amounts": analytics["amount"].isna().sum(),
        "null_users": analytics["user_id"].isna().sum(),
    }

    log.info("Summary Statistics:")
    for key, val in summary.items():
        if isinstance(val, float):
            log.info("  %s: %.2f", key, val)
        else:
            log.info("  %s: %s", key, val)

    # 7. Write analytics table
    log.info("Writing analytics table")
    write_parquet(analytics, p.processed / "analytics_table.parquet")
    log.info("Wrote analytics table: %s", p.processed / "analytics_table.parquet")

    # 8. Build revenue by country summary
    log.info("Building revenue by country summary")
    revenue_summary = (
        analytics
        .groupby("country", dropna=False)
        .agg(
            orders=("order_id", "size"),
            revenue=("amount", "sum"),
            avg_order=("amount", "mean"),
        )
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    
    log.info("Revenue by Country:")
    log.info("\n%s", revenue_summary.to_string(index=False))
    
    # Save revenue summary
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    revenue_path = reports_dir / "revenue_by_country.csv"
    revenue_summary.to_csv(revenue_path, index=False)
    log.info("Saved revenue summary: %s", revenue_path)

    # 9. Display column info
    log.info("Analytics table schema:")
    log.info("  Columns: %s", list(analytics.columns))
    log.info("  Shape: %s rows x %s columns", len(analytics), len(analytics.columns))
    log.info("SUCCESS: Day 3 analytics pipeline complete")


if __name__ == "__main__":
    main()
