"""
EtherScan Gas Tracker API Integration Module

This module provides functions to interact with the EtherScan Gas Tracker API
to retrieve historical gas fee data for Ethereum and other networks.
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any

class EtherScanAPI:
    """
    A class to interact with the EtherScan API for retrieving gas fee data.
    """
    
    def __init__(self, api_key: Optional[str] = None, 
                cache_dir: str = "../data/raw/etherscan"):
        """
        Initialize the EtherScan API client.
        
        Args:
            api_key: EtherScan API key (optional, but recommended for higher rate limits)
            cache_dir: Directory to store cached API responses
        """
        self.api_key = api_key or os.environ.get("ETHERSCAN_API_KEY", "")
        self.cache_dir = cache_dir
        self.base_urls = {
            "ethereum": "https://api.etherscan.io/api",
            "arbitrum": "https://api.arbiscan.io/api",
            "base": "https://api.basescan.org/api",
            "avalanche": "https://api.snowtrace.io/api",
            "polygon": "https://api.polygonscan.com/api",
            "optimism": "https://api-optimistic.etherscan.io/api",
        }
        os.makedirs(cache_dir, exist_ok=True)
    
    def _make_request(self, network: str, params: Dict, 
                     cache_file: Optional[str] = None, cache_ttl: int = 3600) -> Dict:
        """
        Make a request to the EtherScan API with caching.
        
        Args:
            network: Blockchain network (ethereum, arbitrum, etc.)
            params: Query parameters
            cache_file: File name for caching the response
            cache_ttl: Cache time-to-live in seconds (default: 1 hour)
            
        Returns:
            API response as dictionary
        """
        if network not in self.base_urls:
            raise ValueError(f"Unsupported network: {network}")
            
        base_url = self.base_urls[network]
        
        if cache_file:
            cache_path = os.path.join(self.cache_dir, cache_file)
            
            # Check if cache exists and is fresh
            if os.path.exists(cache_path):
                file_age = time.time() - os.path.getmtime(cache_path)
                if file_age < cache_ttl:
                    try:
                        with open(cache_path, 'r') as f:
                            return json.load(f)
                    except (json.JSONDecodeError, IOError):
                        # Cache is invalid, continue with request
                        pass
        
        # Add API key if available
        if self.api_key:
            params["apikey"] = self.api_key
        
        # Make the API request
        response = requests.get(base_url, params=params)
        
        # Check for API errors
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}")
        
        data = response.json()
        
        # Check for API error messages
        if data.get("status") == "0" and data.get("message") != "No transactions found":
            raise Exception(f"API error: {data.get('message', 'Unknown error')}")
        
        # Cache the response
        if cache_file:
            with open(cache_path, 'w') as f:
                json.dump(data, f)
        
        return data
    
    def get_gas_oracle(self, network: str = "ethereum") -> Dict:
        """
        Get current gas prices from the gas oracle.
        
        Args:
            network: Blockchain network
            
        Returns:
            Current gas price data
        """
        params = {
            "module": "gastracker",
            "action": "gasoracle"
        }
        
        cache_file = f"{network}_gas_oracle.json"
        data = self._make_request(network, params, cache_file=cache_file, cache_ttl=300)  # Short TTL for current prices
        
        return data
    
    def get_daily_average_gas_price(self, start_date: datetime, end_date: datetime, 
                                  network: str = "ethereum") -> pd.DataFrame:
        """
        Get daily average gas prices for a date range.
        
        Args:
            start_date: Start date
            end_date: End date
            network: Blockchain network
            
        Returns:
            DataFrame with daily average gas prices
        """
        # Convert dates to timestamps
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())
        
        # Prepare cache file name
        date_range = f"{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}"
        cache_file = f"{network}_daily_gas_{date_range}.json"
        
        # Prepare API parameters
        params = {
            "module": "stats",
            "action": "dailyavggasprice",
            "startdate": start_timestamp,
            "enddate": end_timestamp
        }
        
        try:
            data = self._make_request(network, params, cache_file=cache_file)
            
            if "result" in data and data["result"]:
                # Convert to DataFrame
                df = pd.DataFrame(data["result"])
                
                # Convert timestamp to datetime
                df["timestamp"] = pd.to_datetime(df["unixTimeStamp"], unit="s")
                
                # Convert gas price from Wei to Gwei
                df["gas_price_gwei"] = pd.to_numeric(df["avgGasPrice_Wei"]) / 1e9
                
                return df
            else:
                print(f"No gas price data available for the specified date range on {network}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error retrieving daily average gas prices: {e}")
            return pd.DataFrame()
    
    def get_historical_gas_prices(self, days: int = 365, 
                                network: str = "ethereum") -> pd.DataFrame:
        """
        Get historical gas prices for the specified number of days.
        
        Args:
            days: Number of days of historical data
            network: Blockchain network
            
        Returns:
            DataFrame with historical gas prices
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        return self.get_daily_average_gas_price(start_date, end_date, network)
    
    def get_gas_prices_multi_network(self, days: int = 365, 
                                   networks: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Get historical gas prices for multiple networks.
        
        Args:
            days: Number of days of historical data
            networks: List of networks to query (defaults to all supported networks)
            
        Returns:
            Dictionary mapping network names to DataFrames with gas price data
        """
        if networks is None:
            networks = list(self.base_urls.keys())
        
        results = {}
        
        for network in networks:
            try:
                df = self.get_historical_gas_prices(days, network)
                if not df.empty:
                    results[network] = df
            except Exception as e:
                print(f"Error retrieving gas prices for {network}: {e}")
        
        return results
    
    def compare_network_fees(self, transaction_type: str = "standard") -> pd.DataFrame:
        """
        Compare current gas fees across different networks.
        
        Args:
            transaction_type: Type of transaction (standard, fast, fastest)
            
        Returns:
            DataFrame comparing gas fees across networks
        """
        results = []
        
        for network in self.base_urls.keys():
            try:
                data = self.get_gas_oracle(network)
                
                if "result" in data:
                    result = data["result"]
                    
                    # Extract the appropriate gas price based on transaction type
                    if transaction_type == "standard":
                        gas_price = result.get("SafeGasPrice", result.get("suggestBaseFee", "N/A"))
                    elif transaction_type == "fast":
                        gas_price = result.get("ProposeGasPrice", result.get("suggestBaseFee", "N/A"))
                    elif transaction_type == "fastest":
                        gas_price = result.get("FastGasPrice", result.get("suggestBaseFee", "N/A"))
                    else:
                        gas_price = "N/A"
                    
                    results.append({
                        "network": network,
                        "gas_price_gwei": gas_price,
                        "timestamp": datetime.now()
                    })
            except Exception as e:
                print(f"Error retrieving gas oracle data for {network}: {e}")
                results.append({
                    "network": network,
                    "gas_price_gwei": "Error",
                    "timestamp": datetime.now()
                })
        
        return pd.DataFrame(results)

# Example usage
if __name__ == "__main__":
    api = EtherScanAPI()
    
    # Get current gas prices
    gas_oracle = api.get_gas_oracle()
    print("Current Ethereum Gas Prices:")
    print(f"Safe (Low): {gas_oracle['result']['SafeGasPrice']} Gwei")
    print(f"Proposed (Average): {gas_oracle['result']['ProposeGasPrice']} Gwei")
    print(f"Fast (High): {gas_oracle['result']['FastGasPrice']} Gwei")
    
    # Get historical gas prices for Ethereum
    historical_gas = api.get_historical_gas_prices(days=30)
    print(f"\nHistorical Ethereum Gas Prices (30 days): {len(historical_gas)} data points")
    
    # Compare gas fees across networks
    network_comparison = api.compare_network_fees()
    print("\nNetwork Gas Fee Comparison:")
    print(network_comparison)
