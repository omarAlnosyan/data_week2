import pandas as pd

df = pd.read_parquet('data/processed/orders_clean.parquet')
print('=' * 60)
print('TASK 6 VERIFICATION')
print('=' * 60)
print('\n✓ Columns:', df.columns.tolist())
print('\n✓ Status_clean unique values:')
print(df['status_clean'].value_counts(dropna=False))
print('\n✓ Missing flags:')
print(f'  - amount__isna: {df["amount__isna"].sum()} True')
print(f'  - quantity__isna: {df["quantity__isna"].sum()} True')
print('\n✓ Sample data:')
print(df[['status','status_clean','amount__isna','quantity__isna']].head(10).to_string())
