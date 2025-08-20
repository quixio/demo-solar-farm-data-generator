"""
AlphaVantage Stock Data Connection Test

This script tests the connection to AlphaVantage API to fetch daily stock data
for GOOG and AAPL tickers. This is a connection test only - no Kafka integration yet.

NOTE: This is NOT integrated with Quix Streams yet - just a connection test.
"""
import os
import requests
import json
import time
from typing import Dict, List, Any

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class AlphaVantageConnectionTest:
    """
    Test connection to AlphaVantage API and fetch sample stock data.
    """
    
    def __init__(self):
        """Initialize the connection test with API configuration."""
        self.api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY environment variable is required")
        
        self.base_url = "https://www.alphavantage.co/query"
        self.tickers = ["GOOG", "AAPL"]  # Target tickers as specified
        
    def test_connection(self) -> bool:
        """
        Test basic connectivity to AlphaVantage API.
        Returns True if connection is successful, False otherwise.
        """
        print("🔌 Testing AlphaVantage API connection...")
        
        try:
            # Simple test call with a minimal request
            test_params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": "IBM",  # Using IBM as a test symbol (from documentation)
                "outputsize": "compact",
                "apikey": self.api_key
            }
            
            response = requests.get(self.base_url, params=test_params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check if we got an error response
            if "Error Message" in data:
                print(f"❌ API Error: {data['Error Message']}")
                return False
            elif "Note" in data:
                print(f"⚠️ API Note: {data['Note']}")
                return False
            elif "Information" in data:
                print(f"ℹ️ API Information: {data['Information']}")
                return False
            
            print("✅ AlphaVantage API connection successful!")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Connection error: {e}")
            return False
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False
    
    def fetch_daily_stock_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch daily stock data for a specific symbol.
        
        Args:
            symbol: Stock ticker symbol (e.g., 'GOOG', 'AAPL')
            
        Returns:
            Dictionary containing the API response
        """
        print(f"📊 Fetching daily stock data for {symbol}...")
        
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",  # Get latest 100 data points only
            "apikey": self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if "Error Message" in data:
                print(f"❌ API Error for {symbol}: {data['Error Message']}")
                return {}
            elif "Note" in data:
                print(f"⚠️ API Rate Limit for {symbol}: {data['Note']}")
                return {}
            elif "Information" in data:
                print(f"ℹ️ API Information for {symbol}: {data['Information']}")
                return {}
                
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error for {symbol}: {e}")
            return {}
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error for {symbol}: {e}")
            return {}
        except Exception as e:
            print(f"❌ Unexpected error for {symbol}: {e}")
            return {}
    
    def process_and_display_data(self, symbol: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process and display stock data in a readable format.
        
        Args:
            symbol: Stock ticker symbol
            data: Raw API response data
            
        Returns:
            List of processed stock records
        """
        if not data:
            print(f"⚠️ No data to process for {symbol}")
            return []
        
        print(f"\n📈 Processing daily stock data for {symbol}")
        print("=" * 60)
        
        # Extract metadata
        meta_data = data.get("Meta Data", {})
        time_series = data.get("Time Series (Daily)", {})
        
        if not time_series:
            print(f"⚠️ No time series data found for {symbol}")
            return []
        
        print(f"Symbol: {meta_data.get('2. Symbol', 'N/A')}")
        print(f"Last Refreshed: {meta_data.get('3. Last Refreshed', 'N/A')}")
        print(f"Time Zone: {meta_data.get('5. Time Zone', 'N/A')}")
        print(f"Output Size: {meta_data.get('4. Output Size', 'N/A')}")
        
        # Convert to list and sort by date (newest first)
        processed_records = []
        sorted_dates = sorted(time_series.keys(), reverse=True)
        
        print(f"\n📊 Sample Daily Stock Data (Latest 10 records):")
        print("-" * 100)
        print(f"{'Date':<12} {'Open':<10} {'High':<10} {'Low':<10} {'Close':<10} {'Volume':<15}")
        print("-" * 100)
        
        # Process first 10 records for display
        for i, date in enumerate(sorted_dates[:10]):
            record = time_series[date]
            
            processed_record = {
                "symbol": symbol,
                "date": date,
                "open": float(record["1. open"]),
                "high": float(record["2. high"]),
                "low": float(record["3. low"]),
                "close": float(record["4. close"]),
                "volume": int(record["5. volume"]),
                "timestamp": int(time.time() * 1000)  # Current timestamp for processing
            }
            
            processed_records.append(processed_record)
            
            # Display formatted record
            print(f"{date:<12} {processed_record['open']:<10.2f} {processed_record['high']:<10.2f} "
                  f"{processed_record['low']:<10.2f} {processed_record['close']:<10.2f} "
                  f"{processed_record['volume']:<15,}")
        
        print("-" * 100)
        print(f"✅ Processed {len(processed_records)} records for {symbol}\n")
        
        return processed_records
    
    def run_connection_test(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Run the complete connection test for all specified tickers.
        
        Returns:
            Dictionary mapping ticker symbols to their processed data
        """
        print("🚀 Starting AlphaVantage Connection Test")
        print("=" * 80)
        
        # Test basic connection first
        if not self.test_connection():
            print("❌ Connection test failed. Exiting...")
            return {}
        
        all_data = {}
        
        # Fetch data for each ticker
        for i, ticker in enumerate(self.tickers):
            if i > 0:
                # Add delay between requests to respect rate limits
                print("⏳ Waiting 12 seconds to respect API rate limits...")
                time.sleep(12)
            
            raw_data = self.fetch_daily_stock_data(ticker)
            if raw_data:
                processed_data = self.process_and_display_data(ticker, raw_data)
                all_data[ticker] = processed_data
            else:
                print(f"❌ Failed to fetch data for {ticker}")
                all_data[ticker] = []
        
        # Summary
        print("\n📋 Connection Test Summary")
        print("=" * 40)
        total_records = sum(len(records) for records in all_data.values())
        successful_tickers = len([ticker for ticker, records in all_data.items() if records])
        
        print(f"Tickers tested: {len(self.tickers)}")
        print(f"Successful connections: {successful_tickers}")
        print(f"Total records fetched: {total_records}")
        
        if successful_tickers > 0:
            print("✅ Connection test completed successfully!")
        else:
            print("❌ Connection test failed - no data retrieved")
        
        return all_data


def main():
    """
    Main function to run the AlphaVantage connection test.
    This is a connection test only - no Kafka integration yet.
    """
    print("AlphaVantage Stock Data Connection Test")
    print("======================================")
    print("Testing connection to AlphaVantage API for GOOG and AAPL daily stock data")
    print("NOTE: This is a CONNECTION TEST only - not integrated with Quix Streams yet")
    print()
    
    try:
        # Initialize and run the connection test
        test_client = AlphaVantageConnectionTest()
        results = test_client.run_connection_test()
        
        # Display final results
        if results:
            print("\n🎉 Test completed! Sample data structure for future Kafka integration:")
            print("-" * 70)
            for ticker, records in results.items():
                if records:
                    print(f"\nSample record structure for {ticker}:")
                    print(json.dumps(records[0], indent=2))
                    break  # Just show one example
        
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("Please ensure ALPHAVANTAGE_API_KEY environment variable is set.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()