#!/usr/bin/env python3
"""
Script to calculate TVL-weighted average APY for each day across all pools using summary_apy.csv and summary_tvl.csv.
"""

import pandas as pd
from pathlib import Path
import csv

def load_allowed_pools():
    allowed_pools = set()
    with open('pools_1000000.txt', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            allowed_pools.add(row['name'])
    return allowed_pools

def load_summary_data(allowed_pools):
    apy_path = Path('statistics/summary_apy.csv')
    tvl_path = Path('statistics/summary_tvl.csv')
    if not apy_path.exists() or not tvl_path.exists():
        raise FileNotFoundError('summary_apy.csv or summary_tvl.csv not found in statistics/')

    apy_df = pd.read_csv(apy_path)
    tvl_df = pd.read_csv(tvl_path)

    return apy_df, tvl_df

def calculate_weighted_apy(apy_df, tvl_df):
    # Assume both dataframes have the same columns: 'date' + pool names
    pool_cols = [col for col in apy_df.columns if col != 'date']
    results = []
    for i in range(len(apy_df)):
        apys = apy_df.loc[i, pool_cols].astype(float)
        tvls = tvl_df.loc[i, pool_cols].astype(float)
        mask = (tvls > 0) & apys.notna() & tvls.notna()
        apys = apys[mask]
        tvls = tvls[mask]
        total_tvl = tvls.sum()
        if total_tvl > 0:
            weighted_apy = (apys * tvls).sum() / total_tvl
        else:
            weighted_apy = float('nan')
        results.append({
            'date': apy_df.loc[i, 'date'],
            'weighted_apy': round(weighted_apy, 2),
            'total_tvl': round(total_tvl, 2)
        })
    return pd.DataFrame(results)

def main():
    output_dir = Path('statistics')
    output_dir.mkdir(exist_ok=True)

    print('Loading allowed pools...')
    allowed_pools = load_allowed_pools()
    print(f'Loaded {len(allowed_pools)} pools from pools_1000000.txt')

    print('Loading summary data...')
    apy_df, tvl_df = load_summary_data(allowed_pools)
    pool_cols = [col for col in apy_df.columns if col != 'date']
    print(f'Using {len(pool_cols)} pools present in summary files')

    print('Calculating weighted APY...')
    weighted_apy = calculate_weighted_apy(apy_df, tvl_df)

    output_file = output_dir / 'weighted_apy.csv'
    weighted_apy.to_csv(output_file, index=False)
    print(f'\nSaved weighted APY data to {output_file}')

    print('\nSummary Statistics:')
    print(f"Average Weighted APY: {weighted_apy['weighted_apy'].mean():.2f}%")
    print(f"Min Weighted APY: {weighted_apy['weighted_apy'].min():.2f}%")
    print(f"Max Weighted APY: {weighted_apy['weighted_apy'].max():.2f}%")
    print(f"Average Total TVL: ${weighted_apy['total_tvl'].mean():,.2f}")
    print(f"Min Total TVL: ${weighted_apy['total_tvl'].min():,.2f}")
    print(f"Max Total TVL: ${weighted_apy['total_tvl'].max():,.2f}")

if __name__ == "__main__":
    main() 