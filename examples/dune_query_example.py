#!/usr/bin/env python3
"""
Example script demonstrating how to use the Dune Analytics API to fetch data for a specific query.
This example fetches APY and TVL data for Aave v3 on Ethereum for USDC.

Prerequisites:
1. Dune Analytics API key (set as DUNE_API_KEY environment variable)
2. Python packages: dune-client, pandas

Usage:
1. Set your Dune API key as an environment variable:
   export DUNE_API_KEY=your_api_key_here

2. Run the script:
   python dune_query_example.py
"""

import os
import pandas as pd
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PROTOCOL = "aave-v3"
ASSET = "USDC"
CHAIN = "ethereum"
START_DATE = "2024-06-06"  # June 6, 2024
END_DATE = "2025-06-05"    # June 5, 2025
OUTPUT_DIR = "examples/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Example SQL query for Aave v3 on Ethereum for USDC
AAVE_V3_QUERY = """
SELECT
    date_trunc('day', block_time) as date,
    symbol as asset,
    'ethereum' as chain,
    avg(liquidity_rate) * 100 as apy_base,
    avg(reward_apy) as apy_reward,
    avg(liquidity_rate) * 100 + avg(reward_apy) as apy,
    sum(amount_usd) as tvl
FROM
    aave_v3.supply
WHERE
    block_time BETWEEN '{{start_date}}' AND '{{end_date}}'
    AND symbol = '{{asset}}'
    AND blockchain = 'ethereum'
GROUP BY
    1, 2, 3
ORDER BY
    date
"""

def init_dune_client():
    """Initialize and return a Dune Analytics client"""
    # Get API key from environment variable
    api_key = os.environ.get("DUNE_API_KEY")
    
    if not api_key:
        raise ValueError("DUNE_API_KEY environment variable not set. Please set it with your Dune Analytics API key.")
    
    return DuneClient(api_key)

def execute_dune_sql_query(client, sql, parameters=None):
    """Execute a SQL query on Dune Analytics and return the results"""
    logger.info(f"Executing Dune SQL query with parameters {parameters}")
    
    try:
        # Create a new query
        result = client.sql(sql, params=parameters or [])
        return result.get_rows()
    except Exception as e:
        logger.error(f"Error executing Dune SQL query: {str(e)}")
        return None

def main():
    """Main function to demonstrate Dune Analytics data collection"""
    logger.info("Starting Dune Analytics example query")
    
    try:
        # Initialize Dune client
        client = init_dune_client()
        
        # Prepare parameters
        parameters = [
            QueryParameter.text_type(name="asset", value=ASSET),
            QueryParameter.date_type(name="start_date", value=START_DATE),
            QueryParameter.date_type(name="end_date", value=END_DATE)
        ]
        
        # Execute query
        result = execute_dune_sql_query(client, AAVE_V3_QUERY, parameters)
        
        if result:
            # Convert to DataFrame
            df = pd.DataFrame(result)
            
            # Ensure date column is in the correct format
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            
            # Save to CSV
            output_file = os.path.join(OUTPUT_DIR, f"{PROTOCOL}_{ASSET}_{CHAIN}_example.csv")
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            
            # Display sample data
            logger.info("\nSample data:")
            logger.info(df.head().to_string())
            
            # Display statistics
            logger.info("\nStatistics:")
            logger.info(f"Total data points: {len(df)}")
            logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
            logger.info(f"Average APY: {df['apy'].mean():.2f}%")
            logger.info(f"Average TVL: ${df['tvl'].mean():,.2f}")
        else:
            logger.warning("No data returned from query")
        
    except Exception as e:
        logger.error(f"Error in example query: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
