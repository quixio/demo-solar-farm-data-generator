# DEPENDENCIES:
# pip install requests
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_alphavantage():
    """
    Connect to Alpha Vantage API and fetch sample stock data
    """
    
    # Get environment variables
    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    api_url = os.environ.get('ALPHAVANTAGE_API_URL', 'https://www.alphavantage.co')
    
    if not api_key:
        raise ValueError("ALPHAVANTAGE_API_KEY environment variable is not set")
    
    # List of popular stock symbols to fetch data for
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 
               'META', 'NVDA', 'JPM', 'V', 'WMT']
    
    print("=" * 60)
    print("Alpha Vantage API Connection Test")
    print("=" * 60)
    print(f"API URL: {api_url}")
    print(f"Fetching data for 10 stock symbols...")
    print("=" * 60)
    
    items_fetched = 0
    
    for symbol in symbols:
        if items_fetched >= 10:
            break
            
        try:
            # Construct the API endpoint for intraday data
            endpoint = f"{api_url}/query"
            
            # Parameters for the API call
            params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': symbol,
                'interval': '5min',
                'apikey': api_key,
                'outputsize': 'compact'  # Get only the latest 100 data points
            }
            
            print(f"\nFetching data for {symbol}...")
            
            # Make the API request
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            
            # Parse the JSON response
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                print(f"API Error for {symbol}: {data['Error Message']}")
                continue
            
            if 'Note' in data:
                print(f"API Note: {data['Note']}")
                # API rate limit reached, wait a bit
                time.sleep(12)  # Alpha Vantage free tier has 5 calls/minute limit
                continue
            
            # Extract the latest data point
            if 'Time Series (5min)' in data:
                time_series = data['Time Series (5min)']
                meta_data = data.get('Meta Data', {})
                
                # Get the most recent timestamp
                timestamps = list(time_series.keys())
                if timestamps:
                    latest_timestamp = timestamps[0]
                    latest_data = time_series[latest_timestamp]
                    
                    # Create a structured item
                    item = {
                        'item_number': items_fetched + 1,
                        'symbol': symbol,
                        'timestamp': latest_timestamp,
                        'last_refreshed': meta_data.get('3. Last Refreshed', 'N/A'),
                        'time_zone': meta_data.get('6. Time Zone', 'N/A'),
                        'open': float(latest_data.get('1. open', 0)),
                        'high': float(latest_data.get('2. high', 0)),
                        'low': float(latest_data.get('3. low', 0)),
                        'close': float(latest_data.get('4. close', 0)),
                        'volume': int(latest_data.get('5. volume', 0))
                    }
                    
                    items_fetched += 1
                    
                    # Print the item
                    print(f"\n--- Item {items_fetched} ---")
                    print(json.dumps(item, indent=2))
                else:
                    print(f"No time series data available for {symbol}")
            else:
                print(f"Unexpected response format for {symbol}")
                print(f"Available keys: {list(data.keys())}")
            
            # Rate limiting - Alpha Vantage free tier allows 5 API calls per minute
            if items_fetched < 10:
                print("\nWaiting 12 seconds to respect API rate limits...")
                time.sleep(12)
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {str(e)}")
            continue
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response for {symbol}: {str(e)}")
            continue
        except Exception as e:
            print(f"Unexpected error for {symbol}: {str(e)}")
            continue
    
    print("\n" + "=" * 60)
    print(f"Connection test completed. Fetched {items_fetched} items.")
    print("=" * 60)
    
    return items_fetched

def main():
    """
    Main function to run the connection test
    """
    try:
        # Test the connection and fetch sample data
        items_count = connect_to_alphavantage()
        
        if items_count == 0:
            print("\nWARNING: No items were successfully fetched.")
            print("Please check:")
            print("1. Your API key is valid")
            print("2. You haven't exceeded the API rate limits")
            print("3. The stock market is open (data might be limited on weekends/holidays)")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"\nFATAL ERROR: {str(e)}")
        return 1
    finally:
        print("\nConnection test finished.")

if __name__ == "__main__":
    exit(main())