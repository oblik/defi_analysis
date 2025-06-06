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

def load_all_data():
    """Load all CSV files from the data directory"""
    data_dir = Path('data')
    all_data = {}
    
    # Get all CSV files except summary.csv
    csv_files = glob.glob(str(data_dir / '*.csv'))
    csv_files = [f for f in csv_files if not f.endswith('summary.csv')]
    
    # Save pool information
    with open('pools.txt', 'w') as f:
        f.write("id,name\n")
        for file_path in csv_files:
            filename = Path(file_path).stem
            parts = filename.split('_')
            if len(parts) >= 3:
                protocol = parts[0]
                asset = parts[1]
                chain = parts[2]
                pool_name = f"{protocol}_{asset}_{chain}"
                f.write(f"{filename},{pool_name}\n")
    
    for file_path in csv_files:
        try:
            # Extract protocol, asset, and chain from filename
            filename = Path(file_path).stem
            parts = filename.split('_')
            if len(parts) >= 3:
                protocol = parts[0]
                asset = parts[1]
                chain = parts[2]
                
                # Read the CSV file
                df = pd.read_csv(file_path)
                
                # Check if file is empty
                if df.empty:
                    print(f"Empty file: {filename}")
                    continue
                    
                # Check if required columns exist
                required_columns = ['apy', 'apy_base', 'apy_reward']
                if not all(col in df.columns for col in required_columns):
                    print(f"Missing columns in {filename}. Available columns: {df.columns.tolist()}")
                    continue
                
                df['protocol'] = protocol
                df['asset'] = asset
                df['chain'] = chain
                
                all_data[filename] = df
        except Exception as e:
            print(f"Error loading {file_path}: {str(e)}")
            continue
    
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
        
        # Create a summary DataFrame
        summary = pd.DataFrame({
            'date': best['date'],
            'protocol': best['protocol'],
            'asset': best['asset'],
            'chain': best['chain'],
            f'best_{apy_type}': best[apy_type],
            'tvl': best['tvl']
        })
        
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
            'std_apy': df[f'best_{apy_type}'].std()
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

def main():
    # Load all data
    print("Loading data...")
    all_data = load_all_data()
    print(f"Loaded data for {len(all_data)} protocols")
    
    # Calculate pool statistics
    print("\nCalculating pool statistics...")
    pool_stats = calculate_pool_statistics(all_data)
    
    # Save pool statistics
    pool_stats.to_csv('data/pool_statistics.csv', index=False)
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
        print("\nTop 5 protocols:")
        for protocol, percentage in result['top_protocols'].items():
            print(f"{protocol}: {percentage}%")
    
    # Save results to CSV
    for apy_type, df in best_protocols.items():
        output_file = f'data/best_{apy_type}.csv'
        df.to_csv(output_file, index=False)
        print(f"\nSaved {apy_type} results to {output_file}")

if __name__ == "__main__":
    main() 