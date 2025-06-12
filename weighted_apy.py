#!/usr/bin/env python3
"""
Script to calculate TVL-weighted average APY for each day across all pools.
"""

import pandas as pd
import os
from pathlib import Path
import glob
import csv

def load_data():
    """Load all CSV files from the data directory, filtered by pools_1000000.txt"""
    data_dir = Path('data/defillama')
    all_data = {}
    
    # Load allowed pool names from pools_1000000.txt
    allowed_pools = set()
    with open('pools_1000000.txt', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            allowed_pools.add(row['name'])
    
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    # Get all CSV files except summary files
    csv_files = glob.glob(str(data_dir / '*.csv'))
    # Filter out any summary or statistics files
    csv_files = [f for f in csv_files if not any(x in Path(f).name.lower() for x in [
        'summary', 'statistics', 'weighted', 'best', 'pool_stats'
    ])]
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    for file_path in csv_files:
        try:
            filename = Path(file_path).stem
            if filename not in allowed_pools:
                continue  # skip pools not in pools_1000000.txt
            df = pd.read_csv(file_path)
            
            if not df.empty and all(col in df.columns for col in ['date', 'apy', 'tvl']):
                # Clean and validate data
                df['date'] = pd.to_datetime(df['date'])
                df['apy'] = pd.to_numeric(df['apy'], errors='coerce')
                df['tvl'] = pd.to_numeric(df['tvl'], errors='coerce')
                
                # Remove any negative or unreasonable values
                df = df[
                    (df['apy'] >= 0) & 
                    (df['apy'] <= 1000) &  # Max 1000% APY
                    (df['tvl'] > 0)
                ]
                
                # Drop rows with NaN values
                df = df.dropna(subset=['apy', 'tvl'])
                
                if not df.empty:
                    all_data[filename] = df
                    print(f"Successfully loaded {filename} with {len(df)} rows")
                else:
                    print(f"Warning: No valid data in {filename} after cleaning")
            else:
                print(f"Warning: Missing required columns in {filename}")
            
        except Exception as e:
            print(f"Error loading {file_path}: {str(e)}")
            continue
    
    if not all_data:
        raise ValueError("No valid data was loaded from any files")
    
    print(f"\nSuccessfully loaded data for {len(all_data)} protocols (filtered by pools_1000000.txt)")
    return all_data

def calculate_weighted_apy(all_data):
    """Calculate TVL-weighted average APY for each day"""
    # Combine all data
    combined_data = pd.concat(all_data.values(), ignore_index=True)
    
    # Group by date and calculate weighted average
    weighted_apy = combined_data.groupby('date').apply(
        lambda x: (x['apy'] * x['tvl']).sum() / x['tvl'].sum()
    ).reset_index()
    
    weighted_apy.columns = ['date', 'weighted_apy']
    
    # Calculate total TVL for each day
    total_tvl = combined_data.groupby('date')['tvl'].sum().reset_index()
    total_tvl.columns = ['date', 'total_tvl']
    
    # Merge weighted APY with total TVL
    result = pd.merge(weighted_apy, total_tvl, on='date')
    
    # Sort by date
    result = result.sort_values('date')
    
    # Round values for better readability
    result['weighted_apy'] = result['weighted_apy'].round(2)
    result['total_tvl'] = result['total_tvl'].round(2)
    
    return result

def main():
    # Create output directory
    output_dir = Path('statistics')
    output_dir.mkdir(exist_ok=True)
    
    # Load data
    print("Loading data...")
    all_data = load_data()
    
    # Calculate weighted APY
    print("\nCalculating weighted APY...")
    weighted_apy = calculate_weighted_apy(all_data)
    
    # Save results
    output_file = output_dir / 'weighted_apy.csv'
    weighted_apy.to_csv(output_file, index=False)
    print(f"\nSaved weighted APY data to {output_file}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Average Weighted APY: {weighted_apy['weighted_apy'].mean():.2f}%")
    print(f"Min Weighted APY: {weighted_apy['weighted_apy'].min():.2f}%")
    print(f"Max Weighted APY: {weighted_apy['weighted_apy'].max():.2f}%")
    print(f"Average Total TVL: ${weighted_apy['total_tvl'].mean():,.2f}")
    print(f"Min Total TVL: ${weighted_apy['total_tvl'].min():,.2f}")
    print(f"Max Total TVL: ${weighted_apy['total_tvl'].max():,.2f}")

if __name__ == "__main__":
    main() 