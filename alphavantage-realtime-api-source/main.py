# DEPENDENCIES:
# pip install requests
# pip install python-dotenv
# END_DEPENDENCIES

import os
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get environment variables
api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
api_url = os.environ.get('ALPHAVANTAGE_API_URL', 'https://www.alphavantage.co')

def test_alphavantage_connection():
    """
    Test connection to Alpha Vantage API by fetching USD to EUR exchange rate
    and retrieving 10 sample forex data points
    """
    
    if not api_key:
        print("Error: ALPHAVANTAGE_API_KEY environment variable not set")
        return
    
    print("Testing Alpha Vantage API connection...")
    print(f"API URL: {api_url}")
    print("-" * 50)
    
    try:
        # First, get the current USD to EUR exchange rate as requested
        print("\n1. Fetching USD to EUR exchange rate...")
        exchange_rate_url = f"{api_url}/query"
        exchange_params = {
            'function': 'CURRENCY_EXCHANGE_RATE',
            'from_currency': 'USD',
            'to_currency': 'EUR',
            'apikey': api_key
        }
        
        response = requests.get(exchange_rate_url, params=exchange_params)
        response.raise_for_status()
        
        exchange_data = response.json()
        
        if 'Realtime Currency Exchange Rate' in exchange_data:
            rate_info = exchange_data['Realtime Currency Exchange Rate']
            print(f"\nExchange Rate Information:")
            print(f"  From: {rate_info.get('1. From_Currency Code', 'N/A')} ({rate_info.get('2. From_Currency Name', 'N/A')})")
            print(f"  To: {rate_info.get('3. To_Currency Code', 'N/A')} ({rate_info.get('4. To_Currency Name', 'N/A')})")
            print(f"  Exchange Rate: {rate_info.get('5. Exchange Rate', 'N/A')}")
            print(f"  Last Refreshed: {rate_info.get('6. Last Refreshed', 'N/A')}")
            print(f"  Time Zone: {rate_info.get('7. Time Zone', 'N/A')}")
        else:
            print(f"Exchange rate data structure unexpected: {json.dumps(exchange_data, indent=2)}")
        
        print("\n" + "-" * 50)
        
        # Now get 10 sample forex data points from daily time series
        print("\n2. Fetching 10 sample USD/EUR daily forex data points...")
        forex_url = f"{api_url}/query"
        forex_params = {
            'function': 'FX_DAILY',
            'from_symbol': 'USD',
            'to_symbol': 'EUR',
            'outputsize': 'compact',  # Get latest 100 data points
            'apikey': api_key
        }
        
        response = requests.get(forex_url, params=forex_params)
        response.raise_for_status()
        
        forex_data = response.json()
        
        # Check for API errors
        if 'Error Message' in forex_data:
            print(f"API Error: {forex_data['Error Message']}")
            return
        elif 'Note' in forex_data:
            print(f"API Note: {forex_data['Note']}")
            return
        
        # Extract time series data
        if 'Time Series FX (Daily)' in forex_data:
            time_series = forex_data['Time Series FX (Daily)']
            
            # Get metadata
            metadata = forex_data.get('Meta Data', {})
            print(f"\nForex Data Metadata:")
            print(f"  Information: {metadata.get('1. Information', 'N/A')}")
            print(f"  From Symbol: {metadata.get('2. From Symbol', 'N/A')}")
            print(f"  To Symbol: {metadata.get('3. To Symbol', 'N/A')}")
            print(f"  Last Refreshed: {metadata.get('5. Last Refreshed', 'N/A')}")
            print(f"  Time Zone: {metadata.get('6. Time Zone', 'N/A')}")
            
            # Get the first 10 data points
            dates = sorted(list(time_series.keys()), reverse=True)[:10]
            
            print(f"\n10 Most Recent USD/EUR Daily Exchange Rates:")
            print("-" * 50)
            
            for i, date in enumerate(dates, 1):
                data_point = time_series[date]
                print(f"\nData Point {i}:")
                print(f"  Date: {date}")
                print(f"  Open: {data_point.get('1. open', 'N/A')}")
                print(f"  High: {data_point.get('2. high', 'N/A')}")
                print(f"  Low: {data_point.get('3. low', 'N/A')}")
                print(f"  Close: {data_point.get('4. close', 'N/A')}")
                
        else:
            print(f"Unexpected response structure: {json.dumps(forex_data, indent=2)[:500]}...")
            
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Alpha Vantage API: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        print("\n" + "=" * 50)
        print("Connection test completed")

if __name__ == "__main__":
    test_alphavantage_connection()