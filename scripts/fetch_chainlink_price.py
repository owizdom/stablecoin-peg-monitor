"""
Script to fetch Chainlink price feeds for stablecoins using eth_defi library.

This module fetches real-time price data from Chainlink oracles for USDC and USDT,
calculates deviation from the $1.00 peg, and saves the results to CSV.

The script is designed to be reusable for Dagster jobs and dbt pipelines.
"""
import os
import logging
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3
from typing import Optional
from datetime import datetime, timezone
# Removed eth_defi dependency - using web3 directly instead

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Chainlink feed addresses (hardcoded as specified)
CHAINLINK_FEEDS = {
    "USDC": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6",  # USDC/USD
    "USDT": "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D",  # USDT/USD
}

# Token symbols to fetch
TOKEN_SYMBOLS = ["USDC", "USDT"]


def get_web3_connection() -> Web3:
    """
    Initialize Web3 connection using Alchemy RPC URL from environment variables.
    Supports both Streamlit Cloud secrets and local .env files.
    
    Returns:
        Web3 instance connected to Ethereum mainnet
        
    Raises:
        ValueError: If ALCHEMY_RPC_URL is not set
        ConnectionError: If connection to RPC endpoint fails
    """
    # Try to get from Streamlit secrets if available (for Streamlit Cloud)
    rpc_url = None
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'ALCHEMY_RPC_URL' in st.secrets:
            rpc_url = st.secrets['ALCHEMY_RPC_URL']
    except:
        pass
    
    # Fall back to environment variable (for local development)
    if not rpc_url:
        rpc_url = os.getenv("ALCHEMY_RPC_URL")
    
    if not rpc_url:
        raise ValueError("ALCHEMY_RPC_URL not set in environment variables or Streamlit secrets")
    
    try:
        logger.info(f"Connecting to Alchemy RPC endpoint...")
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        
        if not w3.is_connected():
            raise ConnectionError("Failed to connect to RPC endpoint")
        
        # Verify connection by getting latest block
        block_number = w3.eth.block_number
        logger.info(f"Successfully connected to Ethereum mainnet (block: {block_number})")
        
        return w3
        
    except Exception as e:
        logger.error(f"Error connecting to RPC: {str(e)}")
        raise ConnectionError(f"Error connecting to RPC: {str(e)}")


# Chainlink Aggregator ABI (minimal - just the functions we need)
CHAINLINK_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId", "type": "uint80"},
            {"name": "answer", "type": "int256"},
            {"name": "startedAt", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function"
    }
]


def fetch_token_price(w3: Web3, token_symbol: str, feed_address: str) -> Optional[dict]:
    """
    Fetch token price from Chainlink feed using web3 directly.
    
    Args:
        w3: Web3 instance connected to Ethereum
        token_symbol: Token symbol (USDC or USDT)
        feed_address: Chainlink price feed contract address
        
    Returns:
        Dictionary with price data or None if fetch fails
        
    Raises:
        RuntimeError: If price fetch fails after retries
    """
    try:
        logger.info(f"Fetching {token_symbol} price from Chainlink feed: {feed_address}")
        
        # Create contract instance
        contract = w3.eth.contract(address=Web3.to_checksum_address(feed_address), abi=CHAINLINK_ABI)
        
        # Get decimals
        decimals = contract.functions.decimals().call()
        
        # Get latest round data
        round_data = contract.functions.latestRoundData().call()
        answer = round_data[1]  # The price is in the second element
        
        # Convert to float (adjust for decimals)
        price_float = float(answer) / (10 ** decimals)
        
        if price_float <= 0:
            logger.warning(f"Received invalid price ({price_float}) for {token_symbol}")
            return None
        
        # Calculate deviation from $1.00 peg
        deviation_from_peg = price_float - 1.0
        
        # Get current UTC timestamp
        timestamp = int(datetime.now(timezone.utc).timestamp())
        
        logger.info(
            f"  {token_symbol} price: ${price_float:.6f} "
            f"(deviation: {deviation_from_peg:.6f}, {deviation_from_peg*100:.4f}%)"
        )
        
        return {
            "timestamp": timestamp,
            "token_symbol": token_symbol,
            "price": price_float,
            "deviation_from_peg": deviation_from_peg
        }
        
    except Exception as e:
        logger.error(f"Error fetching {token_symbol} price from Chainlink feed: {str(e)}")
        logger.exception("Full error traceback:")
        return None


def fetch_all_chainlink_prices() -> pd.DataFrame:
    """
    Fetch Chainlink price data for all configured tokens.
    
    This function is designed to be reusable for Dagster jobs and dbt pipelines.
    
    Returns:
        DataFrame with columns: timestamp, token_symbol, price, deviation_from_peg
        
    Raises:
        RuntimeError: If no price data could be fetched for any token
    """
    logger.info("Starting Chainlink price fetch for all tokens...")
    
    # Get Web3 connection
    try:
        w3 = get_web3_connection()
    except (ValueError, ConnectionError) as e:
        logger.error(f"Failed to establish Web3 connection: {str(e)}")
        raise
    
    all_data = []
    
    # Fetch prices for each token
    for token_symbol in TOKEN_SYMBOLS:
        if token_symbol not in CHAINLINK_FEEDS:
            logger.warning(f"No feed address configured for {token_symbol}, skipping...")
            continue
        
        feed_address = CHAINLINK_FEEDS[token_symbol]
        
        try:
            price_data = fetch_token_price(w3, token_symbol, feed_address)
            
            if price_data is not None:
                all_data.append(price_data)
                logger.info(f"Successfully fetched {token_symbol} price data")
            else:
                logger.warning(f"Failed to fetch price data for {token_symbol}")
                
        except Exception as e:
            logger.error(f"Unexpected error processing {token_symbol}: {str(e)}")
            logger.exception("Full error traceback:")
            continue
    
    # Check if we got any data
    if not all_data:
        error_msg = "Failed to fetch price data for any token"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    # Create DataFrame
    df = pd.DataFrame(all_data)
    
    # Ensure columns are in the correct order
    df = df[["timestamp", "token_symbol", "price", "deviation_from_peg"]]
    
    # Sort by token_symbol for consistent output
    df = df.sort_values("token_symbol").reset_index(drop=True)
    
    logger.info(f"Successfully fetched price data for {len(df)} token(s)")
    
    return df


def save_to_csv(df: pd.DataFrame, output_dir: str = "data") -> str:
    """
    Save DataFrame to CSV file.
    
    Args:
        df: DataFrame to save
        output_dir: Output directory path (default: "data")
        
    Returns:
        Path to the saved CSV file
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, "chainlink_prices.csv")
    
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Saved price data to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error saving CSV file: {str(e)}")
        raise


def main():
    """
    Main function to fetch Chainlink prices and save to CSV.
    
    This function can be called directly when running the script standalone,
    or imported and used in Dagster jobs or other pipelines.
    """
    try:
        logger.info("=" * 60)
        logger.info("Chainlink Price Fetch Script")
        logger.info("=" * 60)
        
        # Fetch all prices
        df = fetch_all_chainlink_prices()
        
        # Print DataFrame summary
        print("\n" + "=" * 60)
        print("Price Data Summary:")
        print("=" * 60)
        print(df.to_string(index=False))
        print("\n" + "=" * 60)
        
        # Save to CSV
        output_path = save_to_csv(df)
        print(f"\nData saved to: {output_path}")
        
        # Print summary statistics
        print("\nSummary Statistics:")
        print(f"  Total tokens: {len(df)}")
        print(f"  Average price: ${df['price'].mean():.6f}")
        print(f"  Average deviation: {df['deviation_from_peg'].mean():.6f} ({df['deviation_from_peg'].mean()*100:.4f}%)")
        print(f"  Max deviation: {df['deviation_from_peg'].abs().max():.6f} ({df['deviation_from_peg'].abs().max()*100:.4f}%)")
        
        logger.info("Chainlink price fetching completed successfully!")
        
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        print(f"\n❌ Configuration error: {str(e)}")
        print("Please ensure ALCHEMY_RPC_URL is set in your .env file")
        raise
        
    except ConnectionError as e:
        logger.error(f"Connection error: {str(e)}")
        print(f"\n❌ Connection error: {str(e)}")
        print("Please check your ALCHEMY_RPC_URL and network connection")
        raise
        
    except RuntimeError as e:
        logger.error(f"Runtime error: {str(e)}")
        print(f"\n❌ Runtime error: {str(e)}")
        print("Failed to fetch price data. Check logs for details.")
        raise
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.exception("Full error traceback:")
        print(f"\n❌ Unexpected error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
