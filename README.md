# Stablecoin Peg Monitor

A real-time monitoring system for tracking stablecoin peg stability for USDC and USDT. The system uses **Chainlink price feeds** as data sources, with **Dagster** for orchestration, **dbt** for transformations, **Snowflake** for data warehousing, and **Streamlit** for visualization.

## Project Summary

This project provides real-time monitoring and historical analysis of stablecoin peg stability through:

- **Price Feed Monitoring**: Tracks real-time prices from Chainlink oracles
- **Peg Health Metrics**: Measures deviation from $1.00 peg with statistical analysis
- **Z-Score Analysis**: Identifies anomalies using rolling 24-hour statistics

## Architecture

```
┌─────────────────┐
│ Chainlink Feeds │
│  (Price Data)   │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│         Dagster Pipeline                    │
│  ┌──────────────────┐                      │
│  │fetch_chainlink   │                      │
│  │   _price_op      │                      │
│  └────────┬─────────┘                      │
│           │                                 │
│           ▼                                 │
│     (Daily Schedule)                        │
└────────────────────┬────────────────────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │    Snowflake         │
         │  (Data Warehouse)    │
         │                       │
         │  • CHAINLINK_PRICES  │
         └──────────┬────────────┘
                    │
                    ▼
         ┌───────────────────────┐
         │      dbt Models       │
         │                       │
         │  • stablecoin_peg_   │
         │    health            │
         │  • stablecoin_peg_    │
         │    zscore            │
         └──────────┬────────────┘
                    │
                    ▼
         ┌───────────────────────┐
         │   Streamlit Dashboard │
         │                       │
         │  • Peg Deviation      │
         │  • Peg Z-Score        │
         └───────────────────────┘
```

## Technology Stack

- **Data Sources**:
  - **Chainlink Price Feeds**: For real-time stablecoin price data via smart contracts

- **Data Pipeline**:
  - **Dagster**: Orchestration and scheduling of data ingestion jobs
  - **dbt**: SQL transformations and data modeling
  - **Snowflake**: Data warehouse for storing and querying time-series data

- **Visualization**:
  - **Streamlit**: Interactive web dashboard for monitoring and analysis

## Project Structure

```
stablecoin-peg-monitor/
├── dagster_pipeline/      # Dagster orchestration
│   ├── jobs/              # Job definitions
│   ├── ops/               # Data operations
│   └── configs/          # Configuration files
├── dbt/                   # dbt transformations
│   ├── models/           # SQL models
│   └── seeds/            # Seed data
├── streamlit_app/         # Streamlit dashboard
│   └── app.py            # Main dashboard application
├── scripts/               # Data fetching scripts
│   ├── fetch_chainlink_price.py
│   └── utils/            # Utility scripts
│       ├── setup_snowflake_tables.py
│       ├── load_real_data.py
│       └── fetch_data_continuously.py
├── .env                  # Environment variables (create from template)
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Environment Variables

Create a `.env` file in the project root with the following values:

**Alchemy RPC Configuration:**
- `ALCHEMY_RPC_URL` - Alchemy RPC endpoint for Ethereum mainnet (e.g., `https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY`)
- Get your API key from [Alchemy](https://www.alchemy.com/)

**Chainlink feed addresses** (hardcoded in `fetch_chainlink_price.py`):
- USDC/USD: `0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6`
- USDT/USD: `0x3E7d1eAB13ad0104d2750B8863b489D65364e32D`

**Snowflake Configuration:**
- `SNOWFLAKE_ACCOUNT` - Your Snowflake account identifier
- `SNOWFLAKE_USER` - Snowflake user account
- `SNOWFLAKE_PASSWORD` - Snowflake user password
- `SNOWFLAKE_ROLE` - Snowflake role (e.g., "ACCOUNTADMIN", "SYSADMIN")
- `SNOWFLAKE_WAREHOUSE` - Snowflake warehouse name
- `SNOWFLAKE_DATABASE` - Snowflake database name
- `SNOWFLAKE_SCHEMA` - Snowflake schema name

### 3. Setup Snowflake Schema

Run the setup script to create all necessary tables and views:

```bash
python scripts/utils/setup_snowflake_tables.py
```

### 4. Run Dagster Pipeline

Start the Dagster development server to run data ingestion jobs:

```bash
dagster dev
```

This will:
- Start the Dagster UI at `http://localhost:3000`
- Enable the daily schedule for `stablecoin_daily_job`
- Allow manual triggering of jobs
- Monitor job execution and logs

The pipeline runs automatically every 24 hours at midnight UTC, or you can trigger it manually from the Dagster UI.

### 5. Run dbt Transformations

After data has been loaded into Snowflake, run dbt models to create analytical views:

```bash
cd dbt
dbt run
```

This creates the following models:
- `stablecoin_peg_health`: Peg deviation calculations
- `stablecoin_peg_zscore`: Statistical analysis with z-scores

### 6. Run Streamlit Dashboard

Start the Streamlit application:

```bash
streamlit run streamlit_app/app.py
```

The dashboard will be available at `http://localhost:8501`

## Dashboard Overview

The Streamlit dashboard provides two main pages for monitoring stablecoin metrics:

### Page 1: Peg Deviation
- **Price Chart**: Real-time Chainlink price feed visualization
- **Deviation Chart**: Deviation from $1.00 peg over time
- **Status Indicators**: Color-coded peg health (healthy/warning/critical)
- **Recent Updates**: Latest price data with deviation percentages
- Updates every minute for near real-time monitoring

### Page 2: Peg Z-Score
- **Z-Score Heatmap**: Visual representation of statistical anomalies across tokens
- **Z-Score Time Series**: Line chart showing z-score trends
- **Distribution Charts**: Bar charts for z-score and status distributions
- **Rolling Statistics**: 24-hour rolling mean and standard deviation
- Identifies outliers (>2σ) and unusual patterns (>1σ)

### Sidebar Features
- **Token Selector**: Filter views by USDC, USDT, or view all
- **Connection Status**: Real-time Snowflake connection indicator
- **Navigation Guide**: Quick reference for each page

## Data Flow

1. **Data Ingestion** (Dagster):
   - Retrieves price data from Chainlink feeds
   - Loads raw data into Snowflake tables

2. **Data Transformation** (dbt):
   - Computes peg deviation metrics
   - Generates rolling statistics and z-scores

3. **Visualization** (Streamlit):
   - Queries transformed dbt models
   - Renders interactive charts and tables
   - Provides real-time monitoring capabilities

## Utility Scripts

- `scripts/utils/setup_snowflake_tables.py`: Initial Snowflake schema setup
- `scripts/utils/load_real_data.py`: Fetch and load Chainlink price data
- `scripts/utils/fetch_data_continuously.py`: Continuous data fetching script

## Notes

- The Dagster pipeline runs on a daily schedule but can be triggered manually
- dbt models should be run after each Dagster job completes to update analytics
- Streamlit dashboard caches data for performance (5 min TTL for most data, 1 min for prices)
- All data is stored in Snowflake for historical analysis and querying
