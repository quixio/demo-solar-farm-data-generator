# DEPENDENCIES:
# pip install requests
# pip install python-dotenv
# END_DEPENDENCIES

import os
import requests
import json
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Get environment variables
api_key = os.environ.get('ALPHAVANTAGE_API_KEY', 'demo')
api_url = os.environ.get('ALPHAVANTAGE_API_URL', 'https://www.alphavantage.co')

def test_alphavantage_connection():
    """
    Test connection to Alpha Vantage API by fetching USD to EUR exchange rate
    and recent forex data
    """
    
    print("=" * 60)
    print("Testing Alpha Vantage API Connection")
    print("=" * 60)
    
    try:
        # First, get the current exchange rate from USD to EUR
        print("\n1. Fetching current USD to EUR exchange rate...")
        exchange_url = f"{api_url}/query"
        exchange_params = {
            'function': 'CURRENCY_EXCHANGE_RATE',
            'from_currency': 'USD',
            'to_currency': 'EUR',
            'apikey': api_key
        }
        
        response = requests.get(exchange_url, params=exchange_params)
        response.raise_for_status()
        
        exchange_data = response.json()
        
        # Print the exchange rate data
        print("\n--- Exchange Rate Data ---")
        print(json.dumps(exchange_data, indent=2))
        
        # Now fetch daily forex data to get 10 sample items
        print("\n2. Fetching daily USD/EUR forex data for sample items...")
        
        # Add a small delay to avoid rate limiting
        time.sleep(1)
        
        forex_params = {
            'function': 'FX_DAILY',
            'from_symbol': 'USD',
            'to_symbol': 'EUR',
            'apikey': api_key
        }
        
        response = requests.get(exchange_url, params=forex_params)
        response.raise_for_status()
        
        forex_data = response.json()
        
        # Check if we have the time series data
        if 'Time Series FX (Daily)' in forex_data:
            time_series = forex_data['Time Series FX (Daily)']
            
            print(f"\nTotal data points available: {len(time_series)}")
            print("\n--- Sample of 10 Most Recent Daily USD/EUR Rates ---")
            
            # Get the dates and sort them in descending order
            dates = sorted(time_series.keys(), reverse=True)[:10]
            
            for i, date in enumerate(dates, 1):
                data_point = time_series[date]
                print(f"\nItem {i}: {date}")
                print(f"  Open:  {data_point.get('1. open', 'N/A')}")
                print(f"  High:  {data_point.get('2. high', 'N/A')}")
                print(f"  Low:   {data_point.get('3. low', 'N/A')}")
                print(f"  Close: {data_point.get('4. close', 'N/A')}")
                
        elif 'Error Message' in forex_data:
            print(f"\nAPI Error: {forex_data['Error Message']}")
            
        elif 'Note' in forex_data:
            print(f"\nAPI Note (possibly rate limit): {forex_data['Note']}")
            
        else:
            # If daily data is not available, try intraday
            print("\n3. Attempting to fetch intraday USD/EUR data...")
            time.sleep(1)
            
            intraday_params = {
                'function': 'FX_INTRADAY',
                'from_symbol': 'USD',
                'to_symbol': 'EUR',
                'interval': '5min',
                'apikey': api_key
            }
            
            response = requests.get(exchange_url, params=intraday_params)
            response.raise_for_status()
            
            intraday_data = response.json()
            
            if 'Time Series FX (5min)' in intraday_data:
                time_series = intraday_data['Time Series FX (5min)']
                
                print(f"\nTotal intraday data points available: {len(time_series)}")
                print("\n--- Sample of 10 Most Recent 5-min USD/EUR Rates ---")
                
                # Get the timestamps and sort them in descending order
                timestamps = sorted(time_series.keys(), reverse=True)[:10]
                
                for i, timestamp in enumerate(timestamps, 1):
                    data_point = time_series[timestamp]
                    print(f"\nItem {i}: {timestamp}")
                    print(f"  Open:  {data_point.get('1. open', 'N/A')}")
                    print(f"  High:  {data_point.get('2. high', 'N/A')}")
                    print(f"  Low:   {data_point.get('3. low', 'N/A')}")
                    print(f"  Close: {data_point.get('4. close', 'N/A')}")
            else:
                print("\nUnable to fetch time series data. Response:")
                print(json.dumps(intraday_data, indent=2))
        
        print("\n" + "=" * 60)
        print("Connection test completed successfully!")
        print("=" * 60)
        
    except requests.exceptions.RequestException as e:
        print(f"\nError connecting to Alpha Vantage API: {e}")
        
    except json.JSONDecodeError as e:
        print(f"\nError parsing JSON response: {e}")
        
    except Exception as e:
        print(f"\nUnexpected error: {e}")

if __name__ == "__main__":
    test_alphavantage_connection()