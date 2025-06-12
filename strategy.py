import pandas as pd
import os
from pathlib import Path
import glob

def calculate_pool_statistics(all_data):
    """Calculate average APYs and variance for each pool"""
    stats = []
    
    for pool_name, df in all_data.items():
        try:
            # Check if required columns exist
            required_columns = ['apy', 'apy_base', 'apy_reward']
            if not all(col in df.columns for col in required_columns):
                print(f"Missing columns in {pool_name}. Available columns: {df.columns.tolist()}")
                continue
                
            pool_stats = {
                'pool': pool_name,
                'avg_apy': df['apy'].mean(),
                'avg_apy_base': df['apy_base'].mean(),
                'avg_apy_reward': df['apy_reward'].mean(),
                'avg_apy_total': (df['apy_base'] + df['apy_reward']).mean(),
                'var_apy': df['apy'].var(),
                'var_apy_base': df['apy_base'].var(),
                'var_apy_reward': df['apy_reward'].var(),
                'var_apy_total': (df['apy_base'] + df['apy_reward']).var()
            }
            stats.append(pool_stats)
        except Exception as e:
            print(f"Error processing {pool_name}: {str(e)}")
            continue
    
    return pd.DataFrame(stats)

def load_all_data(threshold=None):
    """Load all CSV files from the data directory, filtered by pools_{THRESHOLD}.txt if threshold is set"""
    data_dir = Path('data/defillama')
    all_data = {}

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    # Determine pools file
    pools_file = f"pools_{threshold}.txt" if threshold is not None else "pools.txt"
    if not Path(pools_file).exists():
        raise FileNotFoundError(f"Pools file not found: {pools_file}")

    # Read allowed pool names from pools file
    import csv
    allowed_pools = set()
    stablecoin_pools = set()
    with open(pools_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'is_stablecoin' in row and row['is_stablecoin'].strip().lower() == 'true':
                allowed_pools.add(row['name'])
    # Only pools with is_stablecoin == True are allowed

    # Get all CSV files except summary/statistics
    csv_files = glob.glob(str(data_dir / '*.csv'))
    csv_files = [f for f in csv_files if not any(x in f for x in ['summary', 'statistics'])]

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    print(f"Found {len(csv_files)} CSV files to process")

    for file_path in csv_files:
        filename = Path(file_path).stem
        if filename not in allowed_pools:
            continue  # skip pools not in threshold file
        try:
            parts = filename.split('_')
            if len(parts) >= 3:
                protocol = parts[0]
                asset = parts[1]
                chain = parts[2]
                df = pd.read_csv(file_path)
                if df.empty:
                    print(f"Warning: Empty file: {filename}")
                    continue
                required_columns = ['date', 'apy', 'apy_base', 'apy_reward', 'tvl']
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    print(f"Warning: Missing columns in {filename}: {missing_columns}")
                    continue
                df['protocol'] = protocol
                df['asset'] = asset
                df['chain'] = chain
                df['date'] = pd.to_datetime(df['date'])
                for col in ['apy', 'apy_base', 'apy_reward', 'tvl']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.dropna(subset=['apy', 'apy_base', 'apy_reward', 'tvl'])
                if not df.empty:
                    all_data[filename] = df
                    print(f"Successfully loaded {filename} with {len(df)} rows")
                else:
                    print(f"Warning: No valid data in {filename} after cleaning")
        except Exception as e:
            print(f"Error loading {file_path}: {str(e)}")
            continue

    if not all_data:
        raise ValueError("No valid data was loaded from any files")

    print(f"\nSuccessfully loaded data for {len(all_data)} protocols (filtered by {pools_file})")
    return all_data

def find_best_protocols(all_data):
    """Find the best protocol for each date and APY type"""
    # Combine all data into one DataFrame
    combined_data = pd.concat(all_data.values(), ignore_index=True)
    
    # Calculate total APY (base + reward)
    combined_data['apy_total'] = combined_data['apy_base'] + combined_data['apy_reward']
    
    # Group by date and find the best protocol for each APY type
    best_protocols = {}
    apy_types = ['apy', 'apy_base', 'apy_reward', 'apy_total']
    
    for apy_type in apy_types:
        # Find the best protocol for each date
        best = combined_data.loc[combined_data.groupby('date')[apy_type].idxmax()]
        
        # Create a summary DataFrame with TVL data
        summary = pd.DataFrame({
            'date': best['date'],
            'protocol': best['protocol'],
            'asset': best['asset'],
            'chain': best['chain'],
            f'best_{apy_type}': best[apy_type],
            'tvl': best['tvl'],
            'tvl_usd': best['tvl'],  # Adding TVL in USD
            'apy_base': best['apy_base'],
            'apy_reward': best['apy_reward']
        })
        
        # Sort by date
        summary = summary.sort_values('date')
        
        best_protocols[apy_type] = summary
    
    return best_protocols

def analyze_strategy(best_protocols):
    """Analyze the strategy results"""
    results = {}
    
    for apy_type, df in best_protocols.items():
        # Calculate statistics
        stats = {
            'mean_apy': df[f'best_{apy_type}'].mean(),
            'min_apy': df[f'best_{apy_type}'].min(),
            'max_apy': df[f'best_{apy_type}'].max(),
            'std_apy': df[f'best_{apy_type}'].std(),
            'mean_tvl': df['tvl_usd'].mean(),
            'min_tvl': df['tvl_usd'].min(),
            'max_tvl': df['tvl_usd'].max(),
            'std_tvl': df['tvl_usd'].std()
        }
        
        # Count protocol appearances
        protocol_counts = df['protocol'].value_counts()
        protocol_percentages = (protocol_counts / len(df) * 100).round(1)
        
        # Get top 5 protocols
        top_protocols = protocol_percentages.head().to_dict()
        
        results[apy_type] = {
            'stats': stats,
            'top_protocols': top_protocols
        }
    
    return results

def main(threshold = 1000000):
    # Set threshold here (e.g., 1000000, 10000000, 50000000)
    print(f"Loading data with threshold {threshold}...")
    all_data = load_all_data(threshold=threshold)
    print(f"Loaded data for {len(all_data)} protocols")
    
    # Calculate pool statistics
    print("\nCalculating pool statistics...")
    pool_stats = calculate_pool_statistics(all_data)
    
    # Save pool statistics
    pool_stats.to_csv('statistics/pool_statistics.csv', index=False)
    print("\nPool Statistics:")
    print(pool_stats.round(2))
    
    # Find best protocols
    print("\nFinding best protocols...")
    best_protocols = find_best_protocols(all_data)
    
    # Analyze results
    print("\nAnalyzing results...")
    results = analyze_strategy(best_protocols)
    
    # Print results
    print("\nResults:")
    for apy_type, result in results.items():
        print(f"\n{apy_type.upper()}:")
        print(f"Mean APY: {result['stats']['mean_apy']:.2f}%")
        print(f"Min APY: {result['stats']['min_apy']:.2f}%")
        print(f"Max APY: {result['stats']['max_apy']:.2f}%")
        print(f"Std APY: {result['stats']['std_apy']:.2f}%")
        print(f"Mean TVL: ${result['stats']['mean_tvl']:,.2f}")
        print(f"Min TVL: ${result['stats']['min_tvl']:,.2f}")
        print(f"Max TVL: ${result['stats']['max_tvl']:,.2f}")
        print(f"Std TVL: ${result['stats']['std_tvl']:,.2f}")
        print("\nTop 5 protocols:")
        for protocol, percentage in result['top_protocols'].items():
            print(f"{protocol}: {percentage}%")
    
    # Save results to CSV
    for apy_type, df in best_protocols.items():
        output_file = f'statistics/best_{apy_type}_{threshold}.csv'
        df.to_csv(output_file, index=False)
        print(f"\nSaved {apy_type} results to {output_file}")

if __name__ == "__main__":
    main(threshold = 1000000) 
    main(threshold = 10000000) 
    main(threshold = 50000000) 