#!/usr/bin/env python3
"""
Script to collect data from Dune Analytics for DeFi protocols from June 6, 2024 to June 5, 2025.

This script uses the Dune Analytics API to fetch APY and TVL data for various DeFi protocols
and assets across different blockchains. It saves the data in CSV format compatible with
the existing data analysis pipeline.

Prerequisites:
1. Dune Analytics API key (set as DUNE_API_KEY environment variable)
2. Python packages: dune-client, pandas, requests

Usage:
1. Set your Dune API key as an environment variable:
   export DUNE_API_KEY=your_api_key_here

2. Run the script:
   python collect_dune_data.py

The script will create a 'data/dune' directory and save CSV files for each protocol-asset-chain
combination, as well as a summary file.
"""

import os
import pandas as pd
import time
import logging
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import Dune client libraries
from dune_client.types import QueryParameter, DuneRecord
from dune_client.client import DuneClient
from dune_client.query import DuneQuery


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dune_data_collection.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
TARGET_PROTOCOLS = ["aave", "fluid", "morpho", 'euler', 'kamino', 'ethena', 'sky.money', 'ondo', 'elixir', 'openeden']
TARGET_ASSETS = ["usdc", "usdt", "usds", "susds", "compound usdt", 'usde', 'usdt0', 'dai']
TARGET_CHAINS = ["ethereum", "base", "arbitrum", "avalanche", "bnb", "polygon"]
START_DATE = "2024-06-06"  # June 6, 2024
END_DATE = "2025-06-05"    # June 5, 2025

# Create output directory
OUTPUT_DIR = "data/dune"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Dune Analytics query IDs and SQL templates
# These are example query IDs and would need to be replaced with actual query IDs from Dune
DUNE_QUERIES = {
    # Aave queries
    "aave-v2": {
        "query_id": None,  # Replace with actual query ID
        "sql_template": """
        SELECT
            date_trunc('day', block_time) as date,
            symbol as asset,
            '{{chain}}' as chain,
            avg(liquidity_rate) * 100 as apy_base,
            avg(incentive_apy) as apy_reward,
            avg(liquidity_rate) * 100 + avg(incentive_apy) as apy,
            sum(amount_usd) as tvl
        FROM
            aave_v2.deposits
        WHERE
            block_time BETWEEN '{{start_date}}' AND '{{end_date}}'
            AND symbol = '{{asset}}'
            AND blockchain = '{{chain}}'
        GROUP BY
            1, 2, 3
        ORDER BY
            date
        """
    },
    "aave-v3": {
        "query_id": None,  # Replace with actual query ID
        "sql_template": """
        SELECT
            date_trunc('day', block_time) as date,
            symbol as asset,
            '{{chain}}' as chain,
            avg(liquidity_rate) * 100 as apy_base,
            avg(reward_apy) as apy_reward,
            avg(liquidity_rate) * 100 + avg(reward_apy) as apy,
            sum(amount_usd) as tvl
        FROM
            aave_v3.supply
        WHERE
            block_time BETWEEN '{{start_date}}' AND '{{end_date}}'
            AND symbol = '{{asset}}'
            AND blockchain = '{{chain}}'
        GROUP BY
            1, 2, 3
        ORDER BY
            date
        """
    },
    # Morpho queries
    "morpho-aave": {
        "query_id": None,  # Replace with actual query ID
        "sql_template": """
        SELECT
            date_trunc('day', block_time) as date,
            token_symbol as asset,
            '{{chain}}' as chain,
            avg(supply_apy) as apy_base,
            avg(reward_apy) as apy_reward,
            avg(supply_apy) + avg(reward_apy) as apy,
            sum(amount_usd) as tvl
        FROM
            morpho.aave_v2_supplies
        WHERE
            block_time BETWEEN '{{start_date}}' AND '{{end_date}}'
            AND token_symbol = '{{asset}}'
            AND blockchain = '{{chain}}'
        GROUP BY
            1, 2, 3
        ORDER BY
            date
        """
    },
    "morpho-compound": {
        "query_id": None,  # Replace with actual query ID
        "sql_template": """
        SELECT
            date_trunc('day', block_time) as date,
            token_symbol as asset,
            '{{chain}}' as chain,
            avg(supply_apy) as apy_base,
            avg(reward_apy) as apy_reward,
            avg(supply_apy) + avg(reward_apy) as apy,
            sum(amount_usd) as tvl
        FROM
            morpho.compound_v2_supplies
        WHERE
            block_time BETWEEN '{{start_date}}' AND '{{end_date}}'
            AND token_symbol = '{{asset}}'
            AND blockchain = '{{chain}}'
        GROUP BY
            1, 2, 3
        ORDER BY
            date
        """
    },
    "morpho-blue": {
        "query_id": None,  # Replace with actual query ID
        "sql_template": """
        SELECT
            date_trunc('day', block_time) as date,
            token_symbol as asset,
            '{{chain}}' as chain,
            avg(supply_apy) as apy_base,
            0 as apy_reward,
            avg(supply_apy) as apy,
            sum(amount_usd) as tvl
        FROM
            morpho.blue_supplies
        WHERE
            block_time BETWEEN '{{start_date}}' AND '{{end_date}}'
            AND token_symbol = '{{asset}}'
            AND blockchain = '{{chain}}'
        GROUP BY
            1, 2, 3
        ORDER BY
            date
        """
    },
    # Euler queries
    "euler-v2": {
        "query_id": None,  # Replace with actual query ID
        "sql_template": """
        SELECT
            date_trunc('day', block_time) as date,
            token_symbol as asset,
            '{{chain}}' as chain,
            avg(supply_apy) as apy_base,
            avg(reward_apy) as apy_reward,
            avg(supply_apy) + avg(reward_apy) as apy,
            sum(amount_usd) as tvl
        FROM
            euler.supplies
        WHERE
            block_time BETWEEN '{{start_date}}' AND '{{end_date}}'
            AND token_symbol = '{{asset}}'
            AND blockchain = '{{chain}}'
        GROUP BY
            1, 2, 3
        ORDER BY
            date
        """
    },
    # Fluid queries
    "fluid-lending": {
        "query_id": None,  # Replace with actual query ID
        "sql_template": """
        SELECT
            date_trunc('day', block_time) as date,
            token_symbol as asset,
            '{{chain}}' as chain,
            avg(supply_apy) as apy_base,
            avg(reward_apy) as apy_reward,
            avg(supply_apy) + avg(reward_apy) as apy,
            sum(amount_usd) as tvl
        FROM
            fluid.supplies
        WHERE
            block_time BETWEEN '{{start_date}}' AND '{{end_date}}'
            AND token_symbol = '{{asset}}'
            AND blockchain = '{{chain}}'
        GROUP BY
            1, 2, 3
        ORDER BY
            date
        """
    },
    # Ethena queries
    "ethena-usde": {
        "query_id": None,  # Replace with actual query ID
        "sql_template": """
        SELECT
            date_trunc('day', block_time) as date,
            token_symbol as asset,
            '{{chain}}' as chain,
            avg(apy) as apy_base,
            0 as apy_reward,
            avg(apy) as apy,
            sum(tvl_usd) as tvl
        FROM
            ethena.staking
        WHERE
            block_time BETWEEN '{{start_date}}' AND '{{end_date}}'
            AND token_symbol = '{{asset}}'
            AND blockchain = '{{chain}}'
        GROUP BY
            1, 2, 3
        ORDER BY
            date
        """
    }
}

# Function to initialize Dune client
def init_dune_client() -> DuneClient:
    """Initialize and return a Dune Analytics client"""
    # Get API key from environment variable
    api_key = os.environ.get("DUNE_API_KEY")
    
    if not api_key:
        raise ValueError("DUNE_API_KEY environment variable not set. Please set it with your Dune Analytics API key.")
    
    return DuneClient(api_key)

# Function to execute a Dune query with SQL
def execute_dune_sql_query(client: DuneClient, sql: str, parameters: List[QueryParameter] = None) -> Optional[List[Dict[str, Any]]]:
    """Execute a SQL query on Dune Analytics and return the results"""
    logger.info(f"Executing Dune SQL query with parameters {parameters}")
    try:
        # Use DuneQuery and client.run_query for raw SQL
        query = DuneQuery(
            query_sql=sql,
            params=parameters or []
        )
        result = client.run_query(query)
        return result.get_rows()
    except Exception as e:
        logger.error(f"Error executing Dune SQL query: {str(e)}")
        return None

# Function to execute a Dune query by ID
def execute_dune_query_by_id(client: DuneClient, query_id: int, parameters: List[QueryParameter] = None) -> Optional[List[Dict[str, Any]]]:
    """Execute a query on Dune Analytics by ID and return the results"""
    logger.info(f"Executing Dune query {query_id} with parameters {parameters}")
    
    try:
        # Execute query by ID using client.query method
        result = client.query(query_id, params=parameters or [])
        return result.get_rows()
    except Exception as e:
        logger.error(f"Error executing Dune query {query_id}: {str(e)}")
        return None

# Function to prepare SQL query with parameters
def prepare_sql_query(template: str, protocol: str, asset: str, chain: str, start_date: str, end_date: str) -> str:
    """Prepare a SQL query by replacing placeholders with actual values"""
    sql = template.replace("{{protocol}}", protocol)
    sql = sql.replace("{{asset}}", asset.upper())
    sql = sql.replace("{{chain}}", chain)
    sql = sql.replace("{{start_date}}", start_date)
    sql = sql.replace("{{end_date}}", end_date)
    return sql

# Function to fetch data for a specific protocol, asset, and chain
def fetch_protocol_data(client: DuneClient, protocol: str, asset: str, chain: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """Fetch data for a specific protocol, asset, and chain"""
    logger.info(f"Fetching data for {protocol} - {asset} on {chain}")
    
    # Check if protocol is supported
    if protocol not in DUNE_QUERIES:
        logger.warning(f"No Dune query configured for protocol {protocol}")
        return None
    
    query_config = DUNE_QUERIES[protocol]
    
    # Prepare parameters
    parameters = [
        QueryParameter.text_type(name="asset", value=asset.upper()),
        QueryParameter.text_type(name="chain", value=chain),
        QueryParameter.date_type(name="start_date", value=start_date),
        QueryParameter.date_type(name="end_date", value=end_date)
    ]
    
    # Execute query
    if query_config["query_id"]:
        # Use query ID if available
        result = execute_dune_query_by_id(client, query_config["query_id"], parameters)
    else:
        # Otherwise use SQL template
        sql = prepare_sql_query(
            query_config["sql_template"],
            protocol,
            asset,
            chain,
            start_date,
            end_date
        )
        result = execute_dune_sql_query(client, sql, parameters)
    
    if not result:
        logger.warning(f"No data returned for {protocol} - {asset} on {chain}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(result)
    
    # Ensure date column is in the correct format
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    
    # Ensure required columns exist
    required_columns = ['date', 'tvl', 'apy', 'apy_base', 'apy_reward']
    for col in required_columns:
        if col not in df.columns:
            if col in ['apy_base', 'apy_reward']:
                # If apy_base or apy_reward is missing but apy exists, derive them
                if 'apy' in df.columns:
                    if col == 'apy_base':
                        df['apy_base'] = df['apy']
                    else:
                        df['apy_reward'] = 0
            else:
                logger.warning(f"Column {col} missing in result for {protocol} - {asset} on {chain}")
                return None
    
    return df

# Function to save data to CSV
def save_to_csv(data: pd.DataFrame, protocol: str, asset: str, chain: str) -> Optional[str]:
    """Save data to a CSV file"""
    if data is None or data.empty:
        logger.warning(f"No data to save for {protocol} - {asset} on {chain}")
        return None
    
    filename = f"{protocol}_{asset}_{chain}".replace(' ', '_')
    filepath = os.path.join(OUTPUT_DIR, f"{filename}.csv")
    
    data.to_csv(filepath, index=False)
    logger.info(f"Saved data to {filepath}")
    
    return filepath

# Function to create a summary file
def create_summary(all_data: Dict[str, pd.DataFrame]) -> None:
    """Create a summary file with dates as rows and pools as columns"""
    if not all_data:
        logger.warning("No data to create summary")
        return
    
    # Get all unique dates
    all_dates = set()
    for df in all_data.values():
        all_dates.update(df['date'].unique())
    
    all_dates = sorted(list(all_dates))
    
    # Create a DataFrame with dates as index and pools as columns
    summary_df = pd.DataFrame(index=all_dates)
    
    # Add APY data for each pool
    for pool_name, df in all_data.items():
        if not df.empty:
            # Set date as index and get APY column
            pool_data = df.set_index('date')['apy']
            summary_df[pool_name] = pool_data
    
    # Fill NaN values with 0
    summary_df = summary_df.fillna(0)
    
    # Save to CSV
    summary_file = os.path.join(OUTPUT_DIR, "summary.csv")
    summary_df.to_csv(summary_file)
    logger.info(f"Summary saved to {summary_file}")

# Function to create pool statistics
def create_pool_statistics(all_data: Dict[str, pd.DataFrame]) -> None:
    """Calculate and save statistics for each pool"""
    if not all_data:
        logger.warning("No data to calculate statistics")
        return
    
    stats = []
    
    for pool_name, df in all_data.items():
        try:
            # Extract protocol, asset, and chain from pool name
            parts = pool_name.split('_')
            protocol = parts[0]
            asset = parts[1]
            chain = parts[2]
            
            # Calculate statistics
            pool_stats = {
                'pool': pool_name,
                'protocol': protocol,
                'asset': asset,
                'chain': chain,
                'avg_apy': df['apy'].mean(),
                'avg_apy_base': df['apy_base'].mean(),
                'avg_apy_reward': df['apy_reward'].mean(),
                'avg_apy_total': (df['apy_base'] + df['apy_reward']).mean(),
                'var_apy': df['apy'].var(),
                'var_apy_base': df['apy_base'].var(),
                'var_apy_reward': df['apy_reward'].var(),
                'var_apy_total': (df['apy_base'] + df['apy_reward']).var(),
                'min_apy': df['apy'].min(),
                'max_apy': df['apy'].max(),
                'avg_tvl': df['tvl'].mean(),
                'min_tvl': df['tvl'].min(),
                'max_tvl': df['tvl'].max(),
                'data_points': len(df)
            }
            stats.append(pool_stats)
        except Exception as e:
            logger.error(f"Error processing statistics for {pool_name}: {str(e)}")
            continue
    
    # Create DataFrame and save to CSV
    stats_df = pd.DataFrame(stats)
    stats_file = os.path.join(OUTPUT_DIR, "pool_statistics.csv")
    stats_df.to_csv(stats_file, index=False)
    logger.info(f"Pool statistics saved to {stats_file}")

# Function to fetch data for all target combinations
def fetch_all_data(client: DuneClient) -> Dict[str, pd.DataFrame]:
    """Fetch data for all target protocol, asset, and chain combinations"""
    all_data = {}
    
    # Create a list of all protocol-asset-chain combinations to fetch
    combinations = []
    for protocol in TARGET_PROTOCOLS:
        for asset in TARGET_ASSETS:
            for chain in TARGET_CHAINS:
                # Check if this protocol exists on this chain
                if is_valid_combination(protocol, asset, chain):
                    combinations.append((protocol, asset, chain))
    
    logger.info(f"Found {len(combinations)} valid protocol-asset-chain combinations")
    
    # Fetch data for each combination
    for i, (protocol, asset, chain) in enumerate(combinations):
        logger.info(f"[{i+1}/{len(combinations)}] Processing {protocol} - {asset} on {chain}")
        
        # Fetch data
        df = fetch_protocol_data(client, protocol, asset, chain, START_DATE, END_DATE)
        
        if df is not None and not df.empty:
            # Save to CSV
            pool_name = f"{protocol}_{asset}_{chain}".replace(' ', '_')
            save_to_csv(df, protocol, asset, chain)
            all_data[pool_name] = df
            logger.info(f"Successfully processed {pool_name} with {len(df)} data points")
        else:
            logger.warning(f"No data for {protocol} - {asset} on {chain}")
        
        # Add a small delay to avoid rate limiting
        time.sleep(1)
    
    return all_data

# Function to check if a protocol-asset-chain combination is valid
def is_valid_combination(protocol: str, asset: str, chain: str) -> bool:
    """Check if a protocol-asset-chain combination is valid"""
    # This is a simplified check. In a real implementation, you would have a more
    # comprehensive list of which protocols support which assets on which chains.
    
    # Example: Aave v3 is not available on BNB Chain
    if protocol == "aave-v3" and chain == "bnb":
        return False
    
    # Example: Ethena is only on Ethereum
    if protocol == "ethena-usde" and chain != "ethereum":
        return False
    
    # Example: USDE is only available on certain protocols
    if asset == "usde" and protocol not in ["aave-v3", "ethena-usde", "morpho-blue"]:
        return False
    
    return True

# Function to check Dune API status
def check_dune_api_status(client: DuneClient) -> bool:
    """Check if the Dune API is accessible"""
    try:
        # Use client.sql for raw SQL
        result = client.sql("SELECT 1 as test")
        return True
    except Exception as e:
        logger.error(f"Error connecting to Dune API: {str(e)}")
        return False

# Main function
def main() -> None:
    """Main function to collect data from Dune Analytics"""
    logger.info("Starting Dune Analytics data collection")
    logger.info(f"Target date range: {START_DATE} to {END_DATE}")
    
    try:
        # Initialize Dune client
        client = init_dune_client()
        
        # Check API status
        if not check_dune_api_status(client):
            logger.error("Failed to connect to Dune API. Please check your API key and internet connection.")
            return
        
        # Fetch data for all target combinations
        all_data = fetch_all_data(client)
        
        if all_data:
            logger.info(f"Successfully collected data for {len(all_data)} pools")
            
            # Create summary file
            create_summary(all_data)
            
            # Create pool statistics
            create_pool_statistics(all_data)
            
            logger.info("Data collection complete!")
        else:
            logger.warning("No data was collected. Please check your queries and parameters.")
        
    except Exception as e:
        logger.error(f"Error in data collection: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
