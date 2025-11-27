"""
Script to continuously fetch real-time Chainlink price data.
Run this script to keep your data fresh - it fetches data every minute.
Press Ctrl+C to stop.
"""
import time
import sys
from load_real_data import load_real_chainlink_data

def fetch_continuously(interval_seconds=60):
    """Fetch data continuously at specified interval."""
    print("=" * 60)
    print("Continuous Data Fetching Started")
    print("=" * 60)
    print(f"Fetching real-time Chainlink prices every {interval_seconds} seconds...")
    print("Press Ctrl+C to stop\n")
    
    fetch_count = 0
    try:
        while True:
            fetch_count += 1
            print(f"\n{'='*60}")
            print(f"Fetch #{fetch_count} - {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")
            
            try:
                load_real_chainlink_data()
                print(f"\n⏳ Waiting {interval_seconds} seconds until next fetch...")
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"❌ Error during fetch: {str(e)}")
                print(f"⏳ Retrying in {interval_seconds} seconds...")
                time.sleep(interval_seconds)
                
    except KeyboardInterrupt:
        print(f"\n\n✅ Stopped after {fetch_count} fetches")
        print("Data fetching stopped by user")

if __name__ == "__main__":
    # Default to 60 seconds, but allow override via command line
    interval = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    fetch_continuously(interval)

