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
from typing import Dict, List, Optional

class EtherScanAPI:
    """
    A class to interact with the EtherScan API for retrieving gas fee data.
    """
    
    def __init__(self, api_key: Optional[str] = None, 
                cache_dir: str = "data/etherscan"):
        """
        Initialize the EtherScan API client.
        
        Args:
            api_key: EtherScan API key (optional, but recommended for higher rate limits)
            cache_dir: Directory to store cached API responses
        """
        self.cache_dir = cache_dir
        self.base_urls = {
            "ethereum": "https://api.etherscan.io/api",
            "arbitrum": "https://api.arbiscan.io/api",
            "base": "https://api.basescan.org/api",
            "avalanche": "https://api.snowtrace.io/api",
            "polygon": "https://api.polygonscan.com/api",
            "optimism": "https://api-optimistic.etherscan.io/api",
        }
        # Create network-specific API key environment variables
        self.api_keys = {
            "ethereum": os.environ.get("ETHERSCAN_API_KEY"),
            "arbitrum": os.environ.get("ARBISCAN_API_KEY"),
            "base": os.environ.get("BASESCAN_API_KEY"),
            "avalanche": os.environ.get("SNOWTRACE_API_KEY"),
            "polygon": os.environ.get("POLYGONSCAN_API_KEY"),
            "optimism": os.environ.get("OPTIMISTIC_ETHERSCAN_API_KEY"),
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
        network_api_key = self.api_keys.get(network)
        if not network_api_key:
            raise ValueError(f"API key is required for {network}. Please set appropriate API key environment variable.")
        
        params["apikey"] = network_api_key
        
        # Make the API request
        response = requests.get(base_url, params=params)
        
        # Check for API errors
        if response.status_code != 200:
            raise Exception(f"API request failed with status code {response.status_code}")
        
        data = response.json()
        
        # Check for API error messages
        if data.get("status") == "0":
            error_msg = data.get("message", "Unknown error")
            if "NOTOK" in error_msg:
                raise ValueError(f"API error for {network}: {error_msg}. Please check your API key.")
            elif data.get("message") != "No transactions found":
                raise Exception(f"API error: {error_msg}")
        
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
        # Note: This endpoint is only available for Ethereum mainnet
        if network != "ethereum":
            print(f"Warning: Daily average gas price is only available for Ethereum mainnet, not {network}")
            return pd.DataFrame()
            
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

def collect_eth_daily_avg_gas_price(
    api_key: str,
    start_date: str,
    end_date: str,
    out_csv: str = "eth_daily_avg_gas.csv"
):
    """
    Collect daily average gas price for Ethereum mainnet from Etherscan and save to CSV.

    Args:
        api_key: Etherscan API key (must be paid for full access)
        start_date: Start date in 'YYYY-MM-DD'
        end_date: End date in 'YYYY-MM-DD'
        out_csv: Output CSV file path
    """
    import requests
    import pandas as pd
    from datetime import datetime

    # Convert to timestamps
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

    url = "https://api.etherscan.io/api"
    params = {
        "module": "stats",
        "action": "dailyavggasprice",
        "startdate": start_ts,
        "enddate": end_ts,
        "apikey": api_key
    }

    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        raise Exception(f"HTTP error: {resp.status_code}")

    data = resp.json()
    if data.get("status") != "1" or "result" not in data:
        raise Exception(f"Etherscan error: {data.get('message', 'Unknown error')}")

    df = pd.DataFrame(data["result"])
    df["date"] = pd.to_datetime(df["unixTimeStamp"], unit="s")
    df["avgGasPrice_Gwei"] = pd.to_numeric(df["avgGasPrice_Wei"]) / 1e9
    df[["date", "avgGasPrice_Gwei"]].to_csv(out_csv, index=False)
    print(f"Saved {len(df)} rows to {out_csv}")

# Example usage
if __name__ == "__main__":
    try:
        api = EtherScanAPI()
        
        # Get current gas prices for Ethereum (this works with the free API key)
        print("Attempting to get current Ethereum gas prices...")
        gas_oracle = api.get_gas_oracle()
        if "result" in gas_oracle:
            print("Current Ethereum Gas Prices:")
            print(f"Safe (Low): {gas_oracle['result']['SafeGasPrice']} Gwei")
            print(f"Proposed (Average): {gas_oracle['result']['ProposeGasPrice']} Gwei")
            print(f"Fast (High): {gas_oracle['result']['FastGasPrice']} Gwei")
        else:
            print(f"Failed to get gas oracle data: {gas_oracle}")
        
        # Note: The following features may require a paid API subscription
        
        # Try to get gas prices for Arbitrum if API key is available
        try:
            print("\nAttempting to get Arbitrum gas prices...")
            arb_gas = api.get_gas_oracle(network="arbitrum")
            if "result" in arb_gas:
                print("Arbitrum Gas Prices:")
                print(f"Gas Price: {arb_gas['result'].get('SafeGasPrice', 'N/A')} Gwei")
            else:
                print("Failed to get Arbitrum gas data")
        except Exception as e:
            print(f"Error with Arbitrum API: {e}")
            print("Note: You may need a separate API key for Arbitrum (ARBISCAN_API_KEY)")
        
        # Try to get historical gas prices for Ethereum
        # Note: This endpoint often requires a paid API subscription
        try:
            print("\nAttempting to get historical Ethereum gas prices...")
            print("Note: This feature often requires a paid API subscription")
            historical_gas = api.get_historical_gas_prices(days=7)  # Try with fewer days
            print(f"Historical Ethereum Gas Prices (7 days): {len(historical_gas)} data points")
            if not historical_gas.empty:
                print(historical_gas.head())
        except Exception as e:
            print(f"Error with historical data: {e}")
            print("This endpoint likely requires a paid API subscription")
        
        print("\nAPI Key Limitations:")
        print("- Free Etherscan API keys have limited access to certain endpoints")
        print("- Historical data and some network data may require paid subscriptions")
        print("- Each blockchain explorer (Etherscan, Arbiscan, etc.) may require separate API keys")
        print("- For production use, consider upgrading to paid API plans")
        
    except Exception as e:
        print(f"Error running example: {e}")
