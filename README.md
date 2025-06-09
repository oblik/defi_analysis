# Liquidity Optimization Across Yield-Generating Protocols

This project analyzes historical APY and TVL data for various DeFi protocols and assets to optimize liquidity allocation across yield-generating protocols.

## Project Overview

The goal of this project is to test the hypothesis that balancing liquidity between existing yield-generating protocols allows users to:

1. Obtain the highest yield at any given time
2. Reduce interest rate volatility
3. Reduce maintenance overhead for users by pooling capital

## Data Collection

The project collects data from two main sources:

1. **DefiLlama API** - Using `collect_defi_data.py`
2. **Dune Analytics** - Using `collect_dune_data.py`

### Supported Protocols

- AAVE (markets: Ethereum, Base, Arbitrum, Avalanche, BNB Chain, Polygon)
- Fluid (markets: Ethereum, Base, Arbitrum, Polygon)
- Morpho (markets: Ethereum, Base, Polygon)
- Euler
- Kamino
- Ethena
- Sky.money
- Ondo
- Elixir
- Openeden

### Supported Assets

- USDC
- USDT
- USDS
- sUSDS
- Compound USDT
- USDE
- USDT0
- DAI

## Setup and Installation

### Prerequisites

- Python 3.8+
- pip (Python package manager)

### Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd Liquidity-Optimization-Across-Yield-Generating-Protocols
   ```

2. Install required packages:
   ```
   pip install pandas requests dune-client
   ```

3. Set up API keys:
   - For Dune Analytics data collection, set your API key as an environment variable:
     ```
     # On Windows
     set DUNE_API_KEY=your_api_key_here
     
     # On macOS/Linux
     export DUNE_API_KEY=your_api_key_here
     ```

## Usage

### Collecting Data from DefiLlama

Run the following command to collect data from DefiLlama:

```
python collect_defi_data.py
```

This will:
- Fetch yield pools from DefiLlama API
- Filter pools based on target protocols, assets, and chains
- Collect historical APY and TVL data for each pool
- Save data to CSV files in the `data` directory

### Collecting Data from Dune Analytics

Run the following command to collect data from Dune Analytics:

```
python collect_dune_data.py
```

This will:
- Connect to Dune Analytics API using your API key
- Execute SQL queries for each protocol-asset-chain combination
- Process and save the data to CSV files in the `data/dune` directory
- Create summary and statistics files

### Analyzing the Data

After collecting data, you can analyze it using:

```
python strategy.py
```

This will:
- Calculate statistics for each pool
- Find the best protocol for each date and APY type
- Analyze the results and print statistics
- Save results to CSV files in the `statistics` directory

## Dune Analytics Setup

To use the Dune Analytics data collection script:

1. Create an account on [Dune Analytics](https://dune.com/)
2. Subscribe to a paid plan to get API access
3. Generate an API key from your account settings
4. Set the API key as an environment variable as described above

### Creating Custom Queries

The `collect_dune_data.py` script includes SQL templates for various protocols. To create your own queries:

1. Log in to Dune Analytics
2. Create a new query using the SQL editor
3. Use the provided SQL templates as a starting point
4. Save your query and note the query ID
5. Update the `DUNE_QUERIES` dictionary in the script with your query ID

## Data Structure

The collected data is organized in two formats:

1. **Individual pool files**
   - Each pool has its own CSV file with daily data
   - Contains: date, TVL, APY, base APY, and reward APY
   - Path: `data/{protocol}_{asset}_{chain}.csv`

2. **Summary file**
   - Matrix format with dates as rows and pools as columns
   - Values represent APY for each pool on each date
   - Missing values filled with zeros
   - Path: `data/summary.csv`

## Project Structure

```
├── collect_defi_data.py     # Script to collect data from DefiLlama
├── collect_dune_data.py     # Script to collect data from Dune Analytics
├── strategy.py              # Script to analyze data and find optimal strategies
├── report.md                # Analysis report
├── pools.txt                # List of tracked pools
├── data/                    # Directory containing collected data
│   ├── aave-v2_*.csv        # Data for Aave v2 pools
│   ├── aave-v3_*.csv        # Data for Aave v3 pools
│   ├── morpho-blue_*.csv    # Data for Morpho Blue pools
│   └── ...                  # Other protocol data
├── best_strategy/           # Directory containing best strategy results
│   ├── best_apy_base.csv    # Best base APY strategy
│   ├── best_apy_reward.csv  # Best reward APY strategy
│   └── best_apy_total.csv   # Best total APY strategy
└── results/                 # Directory containing visualization results
    └── ...                  # Various charts and visualizations
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
