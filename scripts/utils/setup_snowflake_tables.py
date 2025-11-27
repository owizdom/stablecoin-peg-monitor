"""
Script to create all necessary Snowflake tables and views for the stablecoin monitor.
This sets up the schema so the Streamlit app can work even before data is loaded.
"""
import os
from dotenv import load_dotenv
from snowflake.connector import connect

load_dotenv()

def setup_snowflake_schema():
    """Create all necessary tables and views in Snowflake."""
    conn = connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    
    cursor = conn.cursor()
    
    try:
        print("Creating raw data tables...")
        
        # Create CHAINLINK_PRICES table
        create_chainlink_table = """
        CREATE TABLE IF NOT EXISTS CHAINLINK_PRICES (
            TIMESTAMP BIGINT,
            TOKEN VARCHAR(10),
            PRICE DOUBLE,
            DEVIATION_FROM_1 DOUBLE,
            LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(create_chainlink_table)
        print("  Created CHAINLINK_PRICES")
        
        print("\nCreating analytical views...")
        
        # Create stablecoin_peg_health view
        create_peg_health_view = """
        CREATE OR REPLACE VIEW stablecoin_peg_health AS
        SELECT
            TIMESTAMP,
            TOKEN,
            PRICE,
            PRICE - 1.0 AS DEVIATION_FROM_PEG,
            ABS(PRICE - 1.0) AS ABS_DEVIATION_FROM_PEG,
            (PRICE - 1.0) * 100 AS DEVIATION_FROM_PEG_PCT,
            CASE
                WHEN ABS(PRICE - 1.0) <= 0.001 THEN 'healthy'
                WHEN ABS(PRICE - 1.0) <= 0.005 THEN 'warning'
                ELSE 'critical'
            END AS PEG_STATUS,
            LOADED_AT
        FROM CHAINLINK_PRICES
        ORDER BY TOKEN, TIMESTAMP DESC
        """
        cursor.execute(create_peg_health_view)
        print("  Created stablecoin_peg_health view")
        
        # Create stablecoin_peg_zscore table
        create_zscore_table = """
        CREATE OR REPLACE TABLE stablecoin_peg_zscore AS
        WITH peg_data AS (
            SELECT * FROM stablecoin_peg_health
        ),
        rolling_stats AS (
            SELECT
                TIMESTAMP,
                TOKEN,
                PRICE,
                DEVIATION_FROM_PEG,
                AVG(DEVIATION_FROM_PEG) OVER (
                    PARTITION BY TOKEN
                    ORDER BY TIMESTAMP
                    RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
                ) AS ROLLING_MEAN_24H,
                STDDEV(DEVIATION_FROM_PEG) OVER (
                    PARTITION BY TOKEN
                    ORDER BY TIMESTAMP
                    RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
                ) AS ROLLING_STDDEV_24H
            FROM peg_data
        )
        SELECT
            TIMESTAMP,
            TOKEN,
            PRICE,
            DEVIATION_FROM_PEG,
            ROLLING_MEAN_24H,
            ROLLING_STDDEV_24H,
            CASE
                WHEN ROLLING_STDDEV_24H > 0
                THEN (DEVIATION_FROM_PEG - ROLLING_MEAN_24H) / ROLLING_STDDEV_24H
                ELSE 0
            END AS ZSCORE,
            CASE
                WHEN ABS((DEVIATION_FROM_PEG - ROLLING_MEAN_24H) / NULLIF(ROLLING_STDDEV_24H, 0)) > 2 THEN 'outlier'
                WHEN ABS((DEVIATION_FROM_PEG - ROLLING_MEAN_24H) / NULLIF(ROLLING_STDDEV_24H, 0)) > 1 THEN 'unusual'
                ELSE 'normal'
            END AS ZSCORE_STATUS
        FROM rolling_stats
        ORDER BY TOKEN, TIMESTAMP DESC
        """
        cursor.execute(create_zscore_table)
        print("  Created stablecoin_peg_zscore table")
        
        print("\nAll tables and views created successfully!")
        print("\nNext steps:")
        print("  1. Run Dagster pipeline to load data: dagster dev")
        print("  2. Or use scripts/utils/load_real_data.py to fetch and load data")
        print("  3. Refresh Streamlit dashboard to see data")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_snowflake_schema()
