"""
Dagster op to fetch Chainlink prices and load them into Snowflake.
"""
import os
import sys
import pandas as pd
from dotenv import load_dotenv
from dagster import op, Output, MetadataValue
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

# Load environment variables
load_dotenv()

# Add scripts directory to path to import fetch functions
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
from fetch_chainlink_price import fetch_all_chainlink_prices


def get_snowflake_connection():
    """
    Create Snowflake connection from environment variables.
    
    Returns:
        Snowflake connection object
    """
    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )
    return conn


@op
def fetch_chainlink_price_op(context):
    """
    Fetch Chainlink price data for all stablecoins and load into Snowflake.
    
    Returns:
        Dictionary with summary of loaded data
    """
    context.log.info("Starting Chainlink price fetch...")
    
    try:
        # Fetch price data
        df = fetch_all_chainlink_prices()
        
        if df.empty:
            context.log.warning("No price data fetched")
            return Output(
                value={"rows": 0, "status": "empty"},
                metadata={"rows_loaded": MetadataValue.int(0)}
            )
        
        context.log.info(f"Fetched price data for {len(df)} tokens")
        
        # Rename columns to match Snowflake schema
        # New script uses token_symbol and deviation_from_peg
        df = df.rename(columns={
            "token_symbol": "token",
            "deviation_from_peg": "deviation_from_1"
        })
        
        # Remove datetime column if present (keep only timestamp)
        if "datetime" in df.columns:
            df = df.drop(columns=["datetime"])
        
        # Load to Snowflake
        conn = get_snowflake_connection()
        try:
            table_name = "CHAINLINK_PRICES"
            
            # Create table if it doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                TIMESTAMP BIGINT,
                TOKEN VARCHAR(10),
                PRICE DOUBLE,
                DEVIATION_FROM_1 DOUBLE,
                LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            conn.cursor().execute(create_table_sql)
            
            # Write data to Snowflake (append mode)
            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name=table_name,
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                auto_create_table=False,
                overwrite=False
            )
            
            if success:
                context.log.info(f"Successfully loaded {nrows} price records to Snowflake")
                
                # Log price summary
                for _, row in df.iterrows():
                    context.log.info(
                        f"{row['token']}: ${row['price']:.6f} "
                        f"(deviation: {row['deviation_from_1']*100:.4f}%)"
                    )
                
                metadata = {
                    "rows_loaded": MetadataValue.int(nrows),
                    "chunks": MetadataValue.int(nchunks),
                    "tokens": MetadataValue.json(df[["token", "price", "deviation_from_1"]].to_dict("records"))
                }
                
                return Output(
                    value={"rows": nrows, "status": "success", "chunks": nchunks},
                    metadata=metadata
                )
            else:
                raise Exception("Failed to write pandas dataframe to Snowflake")
                
        finally:
            conn.close()
            
    except Exception as e:
        context.log.error(f"Error fetching Chainlink prices: {str(e)}")
        raise

