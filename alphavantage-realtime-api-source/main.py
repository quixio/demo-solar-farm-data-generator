# Cached connection test code for alphavantage realtime api
# Generated on 2025-08-14 17:17:50
# Template: Unknown
# This is cached code - delete this file to force regeneration

# DEPENDENCIES:
# pip install requests
# pip install python-dotenv
# END_DEPENDENCIES

import os
import requests
import json
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get environment variables
api_key = os.environ.get('ALPHAVANTAGE_API_KEY', 'demo')
api_url = os.environ.get('ALPHAVANTAGE_API_URL', 'https://www.alphavantage.co')

def test_alphavantage_forex_connection():
    """
    Test connection to Alpha Vantage API and retrieve USD to EUR exchange rate data.
    Fetches both current exchange rate and historical intraday data.
    """
    
    print("=" * 60)
    print("Testing Alpha Vantage Forex API Connection")
    print("=" * 60)
    print(f"API URL: {api_url}")
    print(f"API Key: {'***' + api_key[-4:] if len(api_key) > 4 else '***'}")
    print("=" * 60)
    
    items_retrieved = 0
    
    try:
        # 1. Get current USD to EUR exchange rate
        print("\n1. Fetching current USD to EUR exchange rate...")
        exchange_rate_url = f"{api_url}/query"
        params = {
            'function': 'CURRENCY_EXCHANGE_RATE',
            'from_currency': 'USD',
            'to_currency': 'EUR',
            'apikey': api_key
        }
        
        response = requests.get(exchange_rate_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if 'Realtime Currency Exchange Rate' in data:
            exchange_data = data['Realtime Currency Exchange Rate']
            items_retrieved += 1
            
            print(f"\n[Item {items_retrieved}] Current Exchange Rate:")
            print(f"  From: {exchange_data.get('1. From_Currency Code', 'N/A')} ({exchange_data.get('2. From_Currency Name', 'N/A')})")
            print(f"  To: {exchange_data.get('3. To_Currency Code', 'N/A')} ({exchange_data.get('4. To_Currency Name', 'N/A')})")
            print(f"  Exchange Rate: {exchange_data.get('5. Exchange Rate', 'N/A')}")
            print(f"  Last Refreshed: {exchange_data.get('6. Last Refreshed', 'N/A')}")
            print(f"  Time Zone: {exchange_data.get('7. Time Zone', 'N/A')}")
            print(f"  Bid Price: {exchange_data.get('8. Bid Price', 'N/A')}")
            print(f"  Ask Price: {exchange_data.get('9. Ask Price', 'N/A')}")
        else:
            print("  No exchange rate data found in response")
            if 'Error Message' in data:
                print(f"  Error: {data['Error Message']}")
            elif 'Note' in data:
                print(f"  Note: {data['Note']}")
        
        # Small delay to respect API rate limits
        time.sleep(1)
        
        # 2. Get intraday forex data (5-minute intervals)
        print("\n2. Fetching intraday USD/EUR forex data (5-min intervals)...")
        intraday_url = f"{api_url}/query"
        params = {
            'function': 'FX_INTRADAY',
            'from_symbol': 'USD',
            'to_symbol': 'EUR',
            'interval': '5min',
            'outputsize': 'compact',  # Get latest 100 data points
            'apikey': api_key
        }
        
        response = requests.get(intraday_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if 'Time Series FX (5min)' in data:
            time_series = data['Time Series FX (5min)']
            
            # Get the most recent 9 data points (we already have 1 from exchange rate)
            timestamps = sorted(time_series.keys(), reverse=True)[:9]
            
            for timestamp in timestamps:
                items_retrieved += 1
                ohlc_data = time_series[timestamp]
                
                print(f"\n[Item {items_retrieved}] Intraday Data - {timestamp}:")
                print(f"  Open:  {ohlc_data.get('1. open', 'N/A')}")
                print(f"  High:  {ohlc_data.get('2. high', 'N/A')}")
                print(f"  Low:   {ohlc_data.get('3. low', 'N/A')}")
                print(f"  Close: {ohlc_data.get('4. close', 'N/A')}")
                
                if items_retrieved >= 10:
                    break
        else:
            print("  No intraday data found in response")
            if 'Error Message' in data:
                print(f"  Error: {data['Error Message']}")
            elif 'Note' in data:
                print(f"  Note: {data['Note']}")
                print("  (API call frequency limit may have been reached)")
            
            # If we can't get intraday data, try daily data as fallback
            if items_retrieved < 10:
                time.sleep(1)
                print("\n3. Attempting to fetch daily USD/EUR forex data as fallback...")
                
                daily_params = {
                    'function': 'FX_DAILY',
                    'from_symbol': 'USD',
                    'to_symbol': 'EUR',
                    'outputsize': 'compact',
                    'apikey': api_key
                }
                
                response = requests.get(intraday_url, params=daily_params)
                response.raise_for_status()
                
                data = response.json()
                
                if 'Time Series FX (Daily)' in data:
                    time_series = data['Time Series FX (Daily)']
                    remaining_items = 10 - items_retrieved
                    timestamps = sorted(time_series.keys(), reverse=True)[:remaining_items]
                    
                    for timestamp in timestamps:
                        items_retrieved += 1
                        ohlc_data = time_series[timestamp]
                        
                        print(f"\n[Item {items_retrieved}] Daily Data - {timestamp}:")
                        print(f"  Open:  {ohlc_data.get('1. open', 'N/A')}")
                        print(f"  High:  {ohlc_data.get('2. high', 'N/A')}")
                        print(f"  Low:   {ohlc_data.get('3. low', 'N/A')}")
                        print(f"  Close: {ohlc_data.get('4. close', 'N/A')}")
                        
                        if items_retrieved >= 10:
                            break
        
        print("\n" + "=" * 60)
        print(f"Connection test completed successfully!")
        print(f"Total items retrieved: {items_retrieved}")
        print("=" * 60)
        
    except requests.exceptions.RequestException as e:
        print(f"\nError connecting to Alpha Vantage API: {e}")
        print("\nPlease check:")
        print("1. Your internet connection")
        print("2. The API URL is correct")
        print("3. Your API key is valid")
        
    except json.JSONDecodeError as e:
        print(f"\nError parsing JSON response: {e}")
        print("Response might not be in expected format")
        
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        
    finally:
        print("\nConnection test finished.")

if __name__ == "__main__":
    test_alphavantage_forex_connection()