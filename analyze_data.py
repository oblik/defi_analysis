#!/usr/bin/env python3
"""
Script to analyze and visualize the collected DeFi protocol data.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

# Configuration
DATA_DIR = "data"
OUTPUT_DIR = "results"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Set plot style
plt.style.use('ggplot')
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 12

def load_all_data():
    """Load all CSV files from the data directory"""
    all_data = {}
    
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.csv') and filename != 'summary.csv':
            filepath = os.path.join(DATA_DIR, filename)
            pool_name = filename.replace('.csv', '')
            
            try:
                df = pd.read_csv(filepath)
                
                # Skip if empty or very few data points
                if len(df) < 5:
                    print(f"Skipping {pool_name} - insufficient data points ({len(df)})")
                    continue
                
                # Convert date to datetime
                df['date'] = pd.to_datetime(df['date'])
                
                # Sort by date
                df = df.sort_values('date')
                
                # Store in dictionary
                all_data[pool_name] = df
                print(f"Loaded {pool_name} - {len(df)} data points")
            except Exception as e:
                print(f"Error loading {pool_name}: {e}")
    
    return all_data

def extract_protocol_info(pool_name):
    """Extract protocol, asset, and chain from pool name"""
    parts = pool_name.split('_')
    
    if len(parts) >= 3:
        protocol = parts[0]
        asset = parts[1]
        chain = parts[2]
    else:
        protocol = parts[0] if len(parts) > 0 else "unknown"
        asset = parts[1] if len(parts) > 1 else "unknown"
        chain = "unknown"
    
    return protocol, asset, chain

def plot_apy_by_protocol(all_data):
    """Plot APY over time for each protocol"""
    protocols = set()
    for pool_name in all_data.keys():
        protocol, _, _ = extract_protocol_info(pool_name)
        protocols.add(protocol)
    
    for protocol in protocols:
        plt.figure(figsize=(14, 8))
        protocol_pools = {name: df for name, df in all_data.items() 
                         if extract_protocol_info(name)[0] == protocol}
        for pool_name, df in protocol_pools.items():
            if 'apy' not in df.columns:
                continue  # skip files without 'apy'
            _, asset, chain = extract_protocol_info(pool_name)
            label = f"{asset} on {chain}"
            plt.plot(df['date'], df['apy'], label=label, alpha=0.7)
        plt.title(f'APY Over Time - {protocol}')
        plt.xlabel('Date')
        plt.ylabel('APY (%)')
        plt.legend(loc='upper right', bbox_to_anchor=(1.15, 1))
        plt.grid(True)
        output_file = os.path.join(OUTPUT_DIR, f'apy_{protocol}.png')
        plt.savefig(output_file, bbox_inches='tight')
        plt.close()
        print(f"Saved APY plot for {protocol} to {output_file}")

def plot_tvl_by_protocol(all_data):
    """Plot TVL over time for each protocol"""
    protocols = set()
    for pool_name in all_data.keys():
        protocol, _, _ = extract_protocol_info(pool_name)
        protocols.add(protocol)
    
    for protocol in protocols:
        plt.figure(figsize=(14, 8))
        
        # Filter pools for this protocol
        protocol_pools = {name: df for name, df in all_data.items() 
                         if extract_protocol_info(name)[0] == protocol}
        
        for pool_name, df in protocol_pools.items():
            _, asset, chain = extract_protocol_info(pool_name)
            label = f"{asset} on {chain}"
            plt.plot(df['date'], df['tvl'] / 1e6, label=label, alpha=0.7)  # Convert to millions
        
        plt.title(f'TVL Over Time - {protocol}')
        plt.xlabel('Date')
        plt.ylabel('TVL (millions USD)')
        plt.legend(loc='upper right', bbox_to_anchor=(1.15, 1))
        plt.grid(True)
        
        # Save figure
        output_file = os.path.join(OUTPUT_DIR, f'tvl_{protocol}.png')
        plt.savefig(output_file, bbox_inches='tight')
        plt.close()
        print(f"Saved TVL plot for {protocol} to {output_file}")

def plot_apy_tvl_by_asset(all_data):
    """Plot APY and TVL over time for each asset"""
    assets = set()
    for pool_name in all_data.keys():
        _, asset, _ = extract_protocol_info(pool_name)
        assets.add(asset)
    
    for asset in assets:
        # Skip assets with too few data points
        asset_pools = {name: df for name, df in all_data.items() 
                      if extract_protocol_info(name)[1] == asset}
        if len(asset_pools) < 2:
            continue
        
        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12), sharex=True)
        
        # Plot APY
        for pool_name, df in asset_pools.items():
            if 'apy' not in df.columns:
                continue  # skip files without 'apy'
            protocol, _, chain = extract_protocol_info(pool_name)
            label = f"{protocol} on {chain}"
            ax1.plot(df['date'], df['apy'], label=label, alpha=0.7)
        
        ax1.set_title(f'APY Over Time - {asset}')
        ax1.set_ylabel('APY (%)')
        ax1.legend(loc='upper right', bbox_to_anchor=(1.15, 1))
        ax1.grid(True)
        
        # Plot TVL
        for pool_name, df in asset_pools.items():
            protocol, _, chain = extract_protocol_info(pool_name)
            label = f"{protocol} on {chain}"
            ax2.plot(df['date'], df['tvl'] / 1e6, label=label, alpha=0.7)  # Convert to millions
        
        ax2.set_title(f'TVL Over Time - {asset}')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('TVL (millions USD)')
        ax2.legend(loc='upper right', bbox_to_anchor=(1.15, 1))
        ax2.grid(True)
        
        plt.tight_layout()
        
        # Save figure
        output_file = os.path.join(OUTPUT_DIR, f'apy_tvl_{asset}.png')
        plt.savefig(output_file, bbox_inches='tight')
        plt.close()
        print(f"Saved APY/TVL plot for {asset} to {output_file}")

def create_aggregated_model(all_data):
    """Create an aggregated model that takes the highest APY at each point in time"""
    # Get all unique dates across all datasets
    all_dates = set()
    for df in all_data.values():
        all_dates.update(df['date'].tolist())
    
    all_dates = sorted(all_dates)
    
    # Create a dataframe with all dates
    agg_df = pd.DataFrame({'date': all_dates})
    
    # For each date, find the pool with the highest APY
    best_apy = []
    best_pool = []
    
    for date in all_dates:
        max_apy = -1
        best_pool_name = None
        
        for pool_name, df in all_data.items():
            # Skip if 'apy' column is missing
            if 'apy' not in df.columns:
                continue
            # Find the closest date in this pool's data
            if date in df['date'].values:
                apy = df.loc[df['date'] == date, 'apy'].values[0]
                if apy > max_apy:
                    max_apy = apy
                    best_pool_name = pool_name
        
        best_apy.append(max_apy if max_apy >= 0 else None)
        best_pool.append(best_pool_name)
    
    agg_df['best_apy'] = best_apy
    agg_df['best_pool'] = best_pool
    
    # Remove rows with no data
    agg_df = agg_df.dropna()
    
    return agg_df

def plot_aggregated_model(agg_df, all_data):
    """Plot the aggregated model showing the highest APY at each point in time"""
    plt.figure(figsize=(14, 8))
    
    # Plot the best APY
    plt.plot(agg_df['date'], agg_df['best_apy'], 'b-', linewidth=2, label='Best APY')
    
    # Plot individual APYs for comparison
    for pool_name, df in all_data.items():
        if pool_name in agg_df['best_pool'].values:
            protocol, asset, chain = extract_protocol_info(pool_name)
            label = f"{protocol} - {asset} on {chain}"
            plt.plot(df['date'], df['apy'], '--', alpha=0.3, linewidth=1)
    
    plt.title('Aggregated Model - Best APY Over Time')
    plt.xlabel('Date')
    plt.ylabel('APY (%)')
    plt.grid(True)
    
    # Add a legend for the best APY
    plt.legend(['Best APY (Aggregated Model)'])
    
    # Save figure
    output_file = os.path.join(OUTPUT_DIR, 'aggregated_model.png')
    plt.savefig(output_file, bbox_inches='tight')
    plt.close()
    print(f"Saved aggregated model plot to {output_file}")
    
    # Also save the data
    agg_csv_file = os.path.join(OUTPUT_DIR, 'aggregated_model.csv')
    agg_df.to_csv(agg_csv_file, index=False)
    print(f"Saved aggregated model data to {agg_csv_file}")
    
    # Calculate statistics
    avg_best_apy = agg_df['best_apy'].mean()
    min_best_apy = agg_df['best_apy'].min()
    max_best_apy = agg_df['best_apy'].max()
    
    # Count occurrences of each pool in the best APY
    pool_counts = agg_df['best_pool'].value_counts()
    
    # Save statistics
    stats_file = os.path.join(OUTPUT_DIR, 'aggregated_model_stats.txt')
    with open(stats_file, 'w') as f:
        f.write(f"Aggregated Model Statistics\n")
        f.write(f"==========================\n\n")
        f.write(f"Average Best APY: {avg_best_apy:.2f}%\n")
        f.write(f"Minimum Best APY: {min_best_apy:.2f}%\n")
        f.write(f"Maximum Best APY: {max_best_apy:.2f}%\n\n")
        f.write(f"Pool Contribution to Best APY:\n")
        
        for pool, count in pool_counts.items():
            protocol, asset, chain = extract_protocol_info(pool)
            percentage = (count / len(agg_df)) * 100
            f.write(f"  {protocol} - {asset} on {chain}: {count} days ({percentage:.1f}%)\n")
    
    print(f"Saved aggregated model statistics to {stats_file}")

def calculate_volatility(all_data, agg_df):
    """Calculate APY volatility for each pool and the aggregated model"""
    volatility_data = []
    
    # Calculate volatility for each pool
    for pool_name, df in all_data.items():
        if len(df) >= 30:  # Only consider pools with sufficient data
            protocol, asset, chain = extract_protocol_info(pool_name)
            
            # Calculate daily APY changes
            df['apy_change'] = df['apy'].diff()
            
            # Calculate volatility (standard deviation of daily changes)
            volatility = df['apy_change'].std()
            
            volatility_data.append({
                'pool': pool_name,
                'protocol': protocol,
                'asset': asset,
                'chain': chain,
                'volatility': volatility,
                'avg_apy': df['apy'].mean(),
                'data_points': len(df)
            })
    
    # Calculate volatility for the aggregated model
    agg_df['apy_change'] = agg_df['best_apy'].diff()
    agg_volatility = agg_df['apy_change'].std()
    
    volatility_data.append({
        'pool': 'Aggregated Model',
        'protocol': 'Aggregated',
        'asset': 'All',
        'chain': 'All',
        'volatility': agg_volatility,
        'avg_apy': agg_df['best_apy'].mean(),
        'data_points': len(agg_df)
    })
    
    # Create a dataframe
    volatility_df = pd.DataFrame(volatility_data)
    
    # Sort by volatility
    volatility_df = volatility_df.sort_values('volatility')
    
    # Save to CSV
    volatility_file = os.path.join(OUTPUT_DIR, 'volatility_analysis.csv')
    volatility_df.to_csv(volatility_file, index=False)
    print(f"Saved volatility analysis to {volatility_file}")
    
    # Plot volatility comparison
    plt.figure(figsize=(14, 8))
    
    # Create a bar chart
    bars = plt.bar(volatility_df['pool'], volatility_df['volatility'])
    
    # Highlight the aggregated model
    for i, bar in enumerate(bars):
        if volatility_df.iloc[i]['pool'] == 'Aggregated Model':
            bar.set_color('red')
    
    plt.title('APY Volatility Comparison')
    plt.xlabel('Pool')
    plt.ylabel('Volatility (Standard Deviation of Daily APY Changes)')
    plt.xticks(rotation=90)
    plt.tight_layout()
    
    # Save figure
    output_file = os.path.join(OUTPUT_DIR, 'volatility_comparison.png')
    plt.savefig(output_file, bbox_inches='tight')
    plt.close()
    print(f"Saved volatility comparison plot to {output_file}")
    
    return volatility_df

def main():
    # Load all data
    print("Loading data...")
    all_data = load_all_data()
    print(f"Loaded {len(all_data)} datasets")
    
    # Plot APY by protocol
    print("\nPlotting APY by protocol...")
    plot_apy_by_protocol(all_data)
    
    # Plot TVL by protocol
    print("\nPlotting TVL by protocol...")
    plot_tvl_by_protocol(all_data)
    
    # Plot APY and TVL by asset
    print("\nPlotting APY and TVL by asset...")
    plot_apy_tvl_by_asset(all_data)
    
    # Create aggregated model
    print("\nCreating aggregated model...")
    agg_df = create_aggregated_model(all_data)
    
    # Plot aggregated model
    print("\nPlotting aggregated model...")
    plot_aggregated_model(agg_df, all_data)
    
    # Calculate volatility
    print("\nCalculating volatility...")
    volatility_df = calculate_volatility(all_data, agg_df)
    
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()

