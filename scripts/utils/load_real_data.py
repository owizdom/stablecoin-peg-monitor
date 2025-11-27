"""
Script to fetch real-time Chainlink price data and load it into Snowflake.
This can be run periodically to keep data fresh.
"""
import os
import sys
import pandas as pd
from dotenv import load_dotenv
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

# Add scripts directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))
from fetch_chainlink_price import fetch_all_chainlink_prices

load_dotenv()

def load_real_chainlink_data():
    """Fetch real Chainlink data and load into Snowflake."""
    print("=" * 60)
    print("Fetching Real-Time Chainlink Price Data")
    print("=" * 60)
    
    # Fetch real data from Chainlink
    df = fetch_all_chainlink_prices()
    
    if df.empty:
        print("❌ No data fetched")
        return
    
    print(f"\n✅ Fetched {len(df)} price records")
    print("\nPrice Summary:")
    for _, row in df.iterrows():
        print(f"  {row['token_symbol']}: ${row['price']:.6f} (deviation: {row['deviation_from_peg']*100:.4f}%)")
    
    # Rename columns to match Snowflake schema
    df = df.rename(columns={
        'token_symbol': 'TOKEN',
        'deviation_from_peg': 'DEVIATION_FROM_1'
    })
    
    # Select only the columns we need
    df = df[['timestamp', 'TOKEN', 'price', 'DEVIATION_FROM_1']]
    df.columns = ['TIMESTAMP', 'TOKEN', 'PRICE', 'DEVIATION_FROM_1']
    
    # Connect to Snowflake
    print("\n" + "=" * 60)
    print("Loading data into Snowflake...")
    print("=" * 60)
    
    conn = connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    
    try:
        # Write to Snowflake (append mode - adds new records)
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name='CHAINLINK_PRICES',
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            auto_create_table=False,
            overwrite=False
        )
        
        if success:
            print(f"\n✅ Successfully loaded {nrows} price records to Snowflake")
            
            # Refresh the views
            cursor = conn.cursor()
            cursor.execute('CREATE OR REPLACE VIEW stablecoin_peg_health AS SELECT TIMESTAMP, TOKEN, PRICE, PRICE - 1.0 AS DEVIATION_FROM_PEG, ABS(PRICE - 1.0) AS ABS_DEVIATION_FROM_PEG, (PRICE - 1.0) * 100 AS DEVIATION_FROM_PEG_PCT, CASE WHEN ABS(PRICE - 1.0) <= 0.001 THEN \'healthy\' WHEN ABS(PRICE - 1.0) <= 0.005 THEN \'warning\' ELSE \'critical\' END AS PEG_STATUS, LOADED_AT FROM CHAINLINK_PRICES ORDER BY TOKEN, TIMESTAMP DESC')
            cursor.execute('CREATE OR REPLACE TABLE stablecoin_peg_zscore AS WITH peg_data AS (SELECT * FROM stablecoin_peg_health), rolling_stats AS (SELECT TIMESTAMP, TOKEN, PRICE, DEVIATION_FROM_PEG, AVG(DEVIATION_FROM_PEG) OVER (PARTITION BY TOKEN ORDER BY TIMESTAMP RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW) AS ROLLING_MEAN_24H, STDDEV(DEVIATION_FROM_PEG) OVER (PARTITION BY TOKEN ORDER BY TIMESTAMP RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW) AS ROLLING_STDDEV_24H FROM peg_data) SELECT TIMESTAMP, TOKEN, PRICE, DEVIATION_FROM_PEG, ROLLING_MEAN_24H, ROLLING_STDDEV_24H, CASE WHEN ROLLING_STDDEV_24H > 0 THEN (DEVIATION_FROM_PEG - ROLLING_MEAN_24H) / ROLLING_STDDEV_24H ELSE 0 END AS ZSCORE, CASE WHEN ABS((DEVIATION_FROM_PEG - ROLLING_MEAN_24H) / NULLIF(ROLLING_STDDEV_24H, 0)) > 2 THEN \'outlier\' WHEN ABS((DEVIATION_FROM_PEG - ROLLING_MEAN_24H) / NULLIF(ROLLING_STDDEV_24H, 0)) > 1 THEN \'unusual\' ELSE \'normal\' END AS ZSCORE_STATUS FROM rolling_stats ORDER BY TOKEN, TIMESTAMP DESC')
            cursor.close()
            
            print("✅ Views refreshed with new data")
            
            # Show data count
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM CHAINLINK_PRICES')
            total_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT TOKEN) FROM CHAINLINK_PRICES')
            token_count = cursor.fetchone()[0]
            cursor.close()
            
            print(f"\n📊 Total records in database: {total_count}")
            print(f"📊 Unique tokens: {token_count}")
            print("\n✅ Real data loaded successfully!")
            
        else:
            print("❌ Failed to load data to Snowflake")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    load_real_chainlink_data()

