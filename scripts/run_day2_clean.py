"""
Task 5: End-to-end cleaning pipeline
Loads raw CSVs, validates, cleans, and writes processed outputs.
Order of operations:
  1. Load and verify columns + non-empty
  2. Enforce schema (types)
  3. Missingness report (observe problems)
  4. Text normalization + flag creation
  5. Write processed output
Warning: Don't validate uniqueness before deduplication.
"""
import logging
import sys
from pathlib import Path

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import (
    enforce_schema,
    missingness_report,
    add_missing_flags,
    normalize_text,
    apply_mapping,
)
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_in_range,
)

log = logging.getLogger(__name__)

# Root of the workspace
ROOT = Path(__file__).parent.parent


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    p = make_paths(ROOT)

    # 1. Load raw inputs
    log.info("Loading raw inputs")
    orders_raw = read_orders_csv(p.raw / "orders.csv")
    users = read_users_csv(p.raw / "users.csv")
    log.info("Rows: orders_raw=%s, users=%s", len(orders_raw), len(users))

    # 2. Verify columns + non-empty (fast)
    log.info("Verifying schema")
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")

    # 3. Enforce schema (types)
    log.info("Enforcing schema")
    orders = enforce_schema(orders_raw)

    # 4. Missingness report (do this early — before you "fix" missing values)
    log.info("Generating missingness report")
    rep = missingness_report(orders)
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    rep_path = reports_dir / "missingness_orders.csv"
    rep.to_csv(rep_path, index=True)
    log.info("Wrote missingness report: %s", rep_path)

    # 5. Text normalization + controlled mapping
    log.info("Normalizing status values")
    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)

    # 6. Add missing flags and create clean version
    log.info("Adding missing flags")
    orders_clean = (
        orders
        .assign(status_clean=status_clean)
        .pipe(add_missing_flags, cols=["amount", "quantity"])
    )

    # 7. Validate amounts and quantities are non-negative (fail fast)
    log.info("Validating ranges")
    assert_in_range(orders_clean["amount"], lo=0, name="amount")
    assert_in_range(orders_clean["quantity"], lo=0, name="quantity")

    # 8. Write processed outputs
    log.info("Writing processed outputs")
    write_parquet(orders_clean, p.processed / "orders_clean.parquet")
    write_parquet(users, p.processed / "users.parquet")
    log.info("Wrote processed outputs to: %s", p.processed)
    log.info("SUCCESS: End-to-end cleaning pipeline complete")


if __name__ == "__main__":
    main()
