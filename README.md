# Liquidity Optimization Across Yield-Generating Protocols

This project is a research and automation toolkit for maximizing DeFi yield by dynamically reallocating liquidity across protocols and chains, based on historical APY and TVL data.

## What It Does

- **Collects** historical APY/TVL data for stablecoin pools from DefiLlama and Dune Analytics.
- **Analyzes** the data to find optimal strategies for yield allocation.
- **Visualizes** protocol, asset, and strategy performance.
- **Outputs** CSVs and charts for further research or integration.

## Supported Protocols & Assets

- **Protocols:** AAVE (v2/v3), Fluid, Morpho (Aave/Compound/Blue), Euler, Kamino, Ethena, Sky.money, Ondo, Elixir, Openeden
- **Chains:** Ethereum, Base, Arbitrum, Avalanche, BNB Chain, Polygon
- **Assets:** USDC, USDT, USDS, sUSDS, Compound USDT, USDE, USDT0, DAI, and more

## Project Structure

```
├── collect_defi_data.py     # DefiLlama data collector
├── collect_dune_data.py     # Dune Analytics data collector
├── collect_etherscan.py     # (Optional) Etherscan data collector
├── strategy.py              # Main analysis/strategy script
├── analyze_data.py          # Visualization and extra analytics
├── weighted_apy.py          # TVL-weighted APY calculation
├── pools_1000000.txt        # List of tracked pools
├── data/
│   ├── defillama/           # Per-pool CSVs from DefiLlama
│   ├── dune/                # Per-pool CSVs from Dune
│   └── etherscan/           # (Optional) Etherscan data
├── statistics/              # Analysis outputs (best strategies, pool stats, summaries)
├── best_strategy/           # Best protocol/asset/chain per day (various APY types)
├── graphs/                  # Visualizations (APY/TVL per protocol/asset, etc)
├── report.md                # Research report (RU)
├── fees.md                  # Fee/transaction cost info
├── requirements.txt         # Python dependencies
└── README.md                # This file
```

## Setup

- Python 3.11+
- `pip install -r requirements.txt`
- For Dune: set `DUNE_API_KEY` env var
- For ETHERSCAN: set `ETHERSCAN_API_KEY` env var

## Usage

**1. Collect DefiLlama data:**
```
python collect_defi_data.py
```
- Fetches, filters, and saves per-pool CSVs in `data/defillama/`

**2. Collect Dune Analytics data:**
```
python collect_dune_data.py
```
- Requires Dune API key
- Saves per-pool CSVs in `data/dune/`

**3. Analyze and strategize:**
```
python strategy.py
```
- Loads all data, computes pool stats, finds best protocol/asset/chain per day, outputs to `statistics/` and `best_strategy/`

**4. Visualize:**
```
python analyze_data.py
```
- Generates APY/TVL charts per protocol/asset, aggregated model, volatility, etc. in `graphs/`

**5. Weighted APY:**
```
python weighted_apy.py
```
- Computes TVL-weighted average APY across all pools

## Data Structure

- **Per-pool CSVs:** `data/defillama/{protocol}_{asset}_{chain}.csv`
  - Columns: date, tvl, apy, apy_base, apy_reward
- **Summary/statistics:** `statistics/`
  - `pool_statistics.csv`, `best_apy_*.csv`, `summary_apy.csv`, `summary_tvl.csv`, etc.
- **Best strategies:** `best_strategy/best_apy_*.csv`
- **Visualizations:** `graphs/`

## Key Insights (from report.md)

- Dynamic allocation to the highest-yielding pool maximizes average APY, but increases volatility.
- Some protocols (e.g., Fluid, Morpho) offer higher but more volatile yields.
- AAVE is more stable but lower-yielding.
- Transaction fees and switching costs can erode gains from frequent rebalancing.
- Diversification and volatility-aware strategies are recommended.

## Fees

See `fees.md` for typical transaction costs by chain.

## Extending

- Add new protocols/assets by editing the config in the data collection scripts.
- Add new Dune queries by updating the `DUNE_QUERIES` dict in `collect_dune_data.py`.

