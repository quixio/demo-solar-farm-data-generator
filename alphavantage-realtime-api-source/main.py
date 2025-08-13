# DEPENDENCIES:
# pip install requests
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import time
from datetime import datetime
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_alphavantage_connection():
    """
    Test connection to Alpha Vantage API and fetch 10 sample data points
    """
    
    # Get environment variables
    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    api_url = os.environ.get('ALPHAVANTAGE_API_URL', 'https://www.alphavantage.co')
    
    if not api_key:
        print("Error: ALPHAVANTAGE_API_KEY environment variable not set")
        return
    
    # List of popular stock symbols to fetch data for
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 
               'META', 'NVDA', 'JPM', 'V', 'WMT']
    
    print("=" * 60)
    print("Testing Alpha Vantage API Connection")
    print("=" * 60)
    print(f"API URL: {api_url}")
    print(f"Fetching real-time data for 10 stock symbols...")
    print("=" * 60)
    
    successful_reads = 0
    
    for i, symbol in enumerate(symbols, 1):
        try:
            # Construct the API endpoint for real-time quote data
            endpoint = f"{api_url}/query"
            
            # Parameters for GLOBAL_QUOTE endpoint (real-time price data)
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': api_key
            }
            
            print(f"\n[{i}/10] Fetching data for {symbol}...")
            
            # Make the API request
            response = requests.get(endpoint, params=params, timeout=10)
            
            # Check if request was successful
            if response.status_code == 200:
                data = response.json()
                
                # Check for API errors
                if 'Error Message' in data:
                    print(f"  ❌ API Error: {data['Error Message']}")
                    continue
                elif 'Note' in data:
                    print(f"  ⚠️  API Note: {data['Note'][:100]}...")
                    # API rate limit reached, wait a bit
                    print("  Waiting 15 seconds due to rate limit...")
                    time.sleep(15)
                    continue
                elif 'Global Quote' in data and data['Global Quote']:
                    quote = data['Global Quote']
                    
                    # Format and print the data
                    print(f"  ✓ Successfully retrieved data:")
                    print(f"    Symbol: {quote.get('01. symbol', 'N/A')}")
                    print(f"    Price: ${quote.get('05. price', 'N/A')}")
                    print(f"    Change: {quote.get('09. change', 'N/A')} ({quote.get('10. change percent', 'N/A')})")
                    print(f"    Volume: {quote.get('06. volume', 'N/A')}")
                    print(f"    Latest Trading Day: {quote.get('07. latest trading day', 'N/A')}")
                    print(f"    Previous Close: ${quote.get('08. previous close', 'N/A')}")
                    print(f"    Open: ${quote.get('02. open', 'N/A')}")
                    print(f"    High: ${quote.get('03. high', 'N/A')}")
                    print(f"    Low: ${quote.get('04. low', 'N/A')}")
                    
                    successful_reads += 1
                else:
                    print(f"  ⚠️  No data available for {symbol}")
                    print(f"  Response: {json.dumps(data, indent=2)[:200]}...")
                    
            else:
                print(f"  ❌ HTTP Error {response.status_code}: {response.text[:100]}")
                
        except requests.exceptions.Timeout:
            print(f"  ❌ Request timeout for {symbol}")
        except requests.exceptions.ConnectionError as e:
            print(f"  ❌ Connection error for {symbol}: {str(e)[:100]}")
        except json.JSONDecodeError:
            print(f"  ❌ Invalid JSON response for {symbol}")
        except Exception as e:
            print(f"  ❌ Unexpected error for {symbol}: {str(e)[:100]}")
        
        # Add a small delay between requests to respect rate limits
        # Alpha Vantage free tier allows 5 API requests per minute
        if i < len(symbols):
            time.sleep(12)  # Wait 12 seconds between requests (5 requests per minute)
    
    print("\n" + "=" * 60)
    print(f"Connection test completed: {successful_reads}/10 successful reads")
    print("=" * 60)
    
    if successful_reads == 0:
        print("\n⚠️  No data was successfully retrieved.")
        print("Possible issues:")
        print("  - Invalid API key")
        print("  - API rate limit exceeded (free tier: 5 requests/minute, 500 requests/day)")
        print("  - Network connectivity issues")
        print("  - API service temporarily unavailable")

if __name__ == "__main__":
    test_alphavantage_connection()