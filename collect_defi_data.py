#!/usr/bin/env python3
"""
Script to collect historical APY and TVL data for specified DeFi protocols and assets.
"""

import requests
import pandas as pd
import json
from datetime import datetime
import time
import os

# Configuration
TARGET_PROTOCOLS = ["aave-v3", "fluid", "morpho", 'euler', 'kamino', 'ethena', 'sky.money', 'ondo', 'elixir', 'openeden'] # для сравнения

TARGET_COINS = ['ethena', 'sky.money', 'ondo', 'elixir', 'openeden']
# Ethena - убрать из общего расчета стратегии, т.к. он не стабильный.
# Посчитать APY на протокол, а не на pool.
TARGET_ASSETS = ["usdc", "usdt", "susds", 'dai' "compound usdt", 'usdt0']
TARGET_CHAINS = ["ethereum", "base", "arbitrum", "avalanche", "bnb", "polygon"]
START_DATE = "2024-06-06"  
END_DATE = "2025-06-06"    

# Create output directory
OUTPUT_DIR = "data/defillama"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Function to pretty print JSON
def print_json(data):
    print(json.dumps(data, indent=2))

def get_all_yield_pools():
    """Get all yield pools from DefiLlama API"""
    print("Fetching all yield pools from DefiLlama...")
    yield_pools_url = "https://yields.llama.fi/pools"
    response = requests.get(yield_pools_url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch yield pools: {response.status_code}")
    
    yield_data = response.json()
    # Save complete response to JSON file
    with open('full_pools.json', 'w') as f:
        json.dump(yield_data, f, indent=2)
    print("Saved complete pool data to full_pools.json")
    
    print(f"Total yield pools: {len(yield_data['data'])}")
    return yield_data['data']

def filter_target_pools(pools, tvl_threshold=1_000_000):
    """Filter pools based on target protocols, assets, chains, and stablecoin status"""
    filtered_pools = []
    
    for pool in pools:
        project = pool.get('project', '').lower()
        symbol = pool.get('symbol', '').lower()
        chain = pool.get('chain', '').lower()
        tvl = pool.get('tvlUsd', 0)
        is_stablecoin = pool.get('stablecoin', False)
        
        if is_stablecoin:
            if any(target in project for target in TARGET_PROTOCOLS):
                if any(asset in symbol for target_asset in TARGET_ASSETS for asset in [target_asset.lower()]):
                    if any(target_chain in chain.lower() for target_chain in TARGET_CHAINS):
                        if tvl > tvl_threshold:  # Filter pools with TVL > threshold
                            filtered_pools.append(pool)
    
    print(f"Found {len(filtered_pools)} matching yield pools with TVL > ${tvl_threshold:,} and stablecoin=True")
    return filtered_pools

def get_historical_data(pool_id):
    """Get historical APY and TVL data for a specific pool"""
    print(f"Fetching historical data for pool {pool_id}...")
    historical_url = f"https://yields.llama.fi/chart/{pool_id}"
    response = requests.get(historical_url)
    
    if response.status_code != 200:
        print(f"Failed to fetch historical data for pool {pool_id}: {response.status_code}")
        return None
    
    historical_data = response.json()
    
    if 'data' not in historical_data or not historical_data['data']:
        print(f"No historical data available for pool {pool_id}")
        return None
    
    return historical_data['data']

def process_historical_data(data, start_date_str, end_date_str):
    """Process historical data and filter by date range"""
    # Create timezone-naive datetime objects for comparison
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    processed_data = []
    
    for point in data:
        # Handle different timestamp formats and ensure timezone-naive comparison
        timestamp = None
        if isinstance(point['timestamp'], int):
            timestamp = datetime.fromtimestamp(point['timestamp'])
        else:
            try:
                # Parse ISO format and convert to naive datetime
                dt = datetime.fromisoformat(point['timestamp'].replace('Z', '+00:00'))
                timestamp = dt.replace(tzinfo=None)
            except:
                try:
                    # Try simple date format
                    timestamp = datetime.strptime(point['timestamp'].split('T')[0], "%Y-%m-%d")
                except:
                    # Skip if we can't parse the timestamp
                    continue
        
        # Filter by date range
        if timestamp and start_date <= timestamp <= end_date:
            processed_data.append({
                'date': timestamp.strftime("%Y-%m-%d"),
                'tvl': point.get('tvlUsd', 0),
                'apy': point.get('apy', 0),  # Convert to percentage
                'apy_base': point.get('apyBase', 0)  if point.get('apyBase') else 0,  # Convert to percentage
                'apy_reward': point.get('apyReward', 0) if point.get('apyReward') else 0  # Convert to percentage
            })
    
    return processed_data

def main():
    # Get all yield pools
    all_pools = get_all_yield_pools()
    print(f"Found {len(all_pools)} yield pools")
    
    # Set TVL threshold here for easy adjustment
    TVL_THRESHOLD = 1_000_000
    # Filter target pools
    target_pools = filter_target_pools(all_pools, tvl_threshold=1000000)
    
    # Save pool IDs to pools.txt
    with open('pools_'+str(TVL_THRESHOLD)+'.txt', 'w') as f:
        f.write('name,pool_id,market,coin,chain,is_stablecoin,address\n')
        for pool in target_pools:
            pool_name = f"{pool['project']}_{pool['symbol']}_{pool['chain']}".replace(' ', '_')
            # Get the pool ID from the chart endpoint
            historical_url = f"https://yields.llama.fi/chart/{pool['pool']}"
            is_stablecoin = pool.get('stablecoin', False)
            # Get the first token address from underlyingTokens
            address = pool.get('underlyingTokens', [''])[0] if pool.get('underlyingTokens') else ''
            f.write(f"{pool_name},{historical_url},{pool['project']},{pool['symbol']},{pool['chain']},{is_stablecoin},{address}\n")
    
    # Display target pools
    print("\nTarget pools:")
    for i, pool in enumerate(target_pools):
        print(f"{i+1}. {pool['project']} - {pool['symbol']} on {pool['chain']}: APY {pool.get('apy', 0):.2f}%, TVL ${pool.get('tvlUsd', 0):,.2f}")
    
    # Collect historical data for each pool
    all_historical_data = {}
    
    for i, pool in enumerate(target_pools):
        pool_id = pool['pool']
        pool_name = f"{pool['project']}_{pool['symbol']}_{pool['chain']}".replace(' ', '_')
        print(f"\n[{i+1}/{len(target_pools)}] Collecting historical data for {pool_name}...")
        
        historical_data = get_historical_data(pool_id)
        
        if historical_data:
            processed_data = process_historical_data(historical_data, START_DATE, END_DATE)
            
            if processed_data:
                print(f"  - Got {len(processed_data)} data points within date range")
                
                # Save to CSV
                df = pd.DataFrame(processed_data)
                csv_file = os.path.join(OUTPUT_DIR, f"{pool_name}.csv")
                df.to_csv(csv_file, index=False)
                print(f"  - Saved to {csv_file}")
                
                # Store in dictionary for aggregation
                all_historical_data[pool_name] = df
            else:
                print("  - No data points within specified date range")
        
        # Add a small delay to avoid rate limiting
        time.sleep(0.5)
    
    # Create a summary file with dates as rows and pools as columns
    all_dates = set()
    for df in all_historical_data.values():
        all_dates.update(df['date'].unique())
    
    all_dates = sorted(list(all_dates))
    
    # Create DataFrames for APY and TVL
    apy_summary = pd.DataFrame(index=all_dates)
    tvl_summary = pd.DataFrame(index=all_dates)
    
    # Add APY and TVL data for each pool
    for pool_name, df in all_historical_data.items():
        if not df.empty:
            # Get pool info from the original data
            pool_info = next((p for p in target_pools if f"{p['project']}_{p['symbol']}_{p['chain']}".replace(' ', '_') == pool_name), None)
            
            # Only include stablecoins
            if pool_info and pool_info.get('stablecoin', False):
                # Set date as index and get APY and TVL columns
                pool_apy = df.set_index('date')['apy']
                pool_tvl = df.set_index('date')['tvl']
                apy_summary[pool_name] = pool_apy
                tvl_summary[pool_name] = pool_tvl
    
    # Fill NaN values with 0
    apy_summary = apy_summary.fillna(0)
    tvl_summary = tvl_summary.fillna(0)
    
    # Save APY summary
    apy_summary_file = os.path.join("statistics/summary_apy.csv")
    apy_summary = apy_summary.reset_index().rename(columns={"index": "date"})
    apy_summary.to_csv(apy_summary_file, index=False)
    print(f"\nAPY Summary saved to {apy_summary_file}")
    
    # Save TVL summary
    tvl_summary_file = os.path.join("statistics/summary_tvl.csv")
    tvl_summary = tvl_summary.reset_index().rename(columns={"index": "date"})
    tvl_summary.to_csv(tvl_summary_file, index=False)
    print(f"TVL Summary saved to {tvl_summary_file}")
    
    print("\nData collection complete!")

if __name__ == "__main__":
    main()

