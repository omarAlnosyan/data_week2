"""
Generate messy dummy data for testing data cleaning pipeline
Creates 1000 users and 5000 orders with intentional data quality issues
"""

import pandas as pd
import numpy as np
from pathlib import Path


def generate_messy_data(n_users=1000, n_orders=5000):
    """
    Generate realistic messy data with quality issues
    
    Args:
        n_users: Number of users to generate
        n_orders: Number of orders to generate
    """
    
    # Setup Path
    data_dir = Path("data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)

    # --- USERS DATA ---
    user_ids = [f"{i:04d}" for i in range(1, n_users + 1)]
    users = pd.DataFrame({
        "user_id": user_ids,
        "country": np.random.choice(["SA", "AE", "KW", "QA"], n_users),
        "signup_date": pd.date_range("2025-01-01", "2025-12-22", periods=n_users).strftime("%Y-%m-%d")
    })
    
    # --- ORDERS DATA ---
    status_options = ["Paid", "paid", "PAID", "Refunded", "refund", "refunded", "Pending"]
    full_date_range = pd.date_range(start="2025-01-01 00:00", end="2025-12-22 23:59", freq="min")

    orders = pd.DataFrame({
        "order_id": [f"A{i:04d}" for i in range(1, n_orders + 1)],
        "user_id": np.random.choice(user_ids, n_orders),
        "amount": np.random.uniform(5.0, 500.0, n_orders).round(2).astype(str),
        "quantity": np.random.randint(1, 10, n_orders).astype(str),
        "created_at": np.random.choice(full_date_range, n_orders),
        "status": np.random.choice(status_options, n_orders)
    })

    # 1. Inject 5% Conflicting Duplicates
    print(f"\n📌 Injecting 5% duplicate rows...")
    num_dupes = int(n_orders * 0.05)
    dupes = orders.iloc[:num_dupes].copy()
    dupes["created_at"] = dupes["created_at"] + pd.Timedelta(hours=5)
    dupes["status"] = "refunded"
    orders = pd.concat([orders, dupes], ignore_index=True).sample(frac=1).reset_index(drop=True)
    print(f"   ✓ Added {num_dupes} duplicate rows")

    # 2. Inject Random Empty Values (NaNs) in 10% of rows
    print(f"\n📌 Injecting 10% missing values...")
    cols_to_mess_up = ["amount", "quantity", "created_at", "status"]
    for col in cols_to_mess_up:
        mask = np.random.random(len(orders)) < 0.10
        orders.loc[mask, col] = np.nan
    print(f"   ✓ Set ~10% of values to NaN in: {cols_to_mess_up}")

    # 3. Format dates to string
    orders["created_at"] = pd.to_datetime(orders["created_at"]).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Save
    print(f"\n📌 Saving data files...")
    users.to_csv(data_dir / "users.csv", index=False)
    orders.to_csv(data_dir / "orders.csv", index=False)
    
    print(f"\n{'='*50}")
    print(f"✅ Created {len(users)} users")
    print(f"✅ Created {len(orders)} orders (with duplicates)")
    print(f"⚠️  Injected quality issues:")
    print(f"   - Conflicting status values (Paid, paid, PAID, etc.)")
    print(f"   - 5% duplicate rows with variations")
    print(f"   - ~10% random missing values (NaN)")
    print(f"{'='*50}")
    print(f"\nFiles saved to:")
    print(f"  - {data_dir / 'users.csv'}")
    print(f"  - {data_dir / 'orders.csv'}")


if __name__ == "__main__":
    generate_messy_data()
