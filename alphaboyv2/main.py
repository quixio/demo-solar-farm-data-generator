import os
import requests
import time
from datetime import datetime, timedelta
import json

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class AlphaVantageForexConnector:
    """
    Connection test for Alpha Vantage Forex API to retrieve Thai Baht (THB) to Euro (EUR) exchange rates.
    
    This is a connection test only - no Kafka integration yet.
    """
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.from_symbol = "THB"  # Thai Baht
        self.to_symbol = "EUR"    # Euro
        self.session = requests.Session()
        # Set timeout and retry parameters
        self.session.timeout = 30
        
    def test_connection(self):
        """Test basic connection to Alpha Vantage API"""
        print("🔗 Testing connection to Alpha Vantage API...")
        
        try:
            # Use CURRENCY_EXCHANGE_RATE for a simple connection test
            test_params = {
                'function': 'CURRENCY_EXCHANGE_RATE',
                'from_currency': self.from_symbol,
                'to_currency': self.to_symbol,
                'apikey': self.api_key
            }
            
            response = self.session.get(self.base_url, params=test_params)
            response.raise_for_status()
            
            data = response.json()
            
            if "Error Message" in data:
                print(f"❌ API Error: {data['Error Message']}")
                return False
            elif "Note" in data:
                print(f"⚠️  API Limit Notice: {data['Note']}")
                return False
            elif "Realtime Currency Exchange Rate" in data:
                print("✅ Connection successful!")
                exchange_info = data["Realtime Currency Exchange Rate"]
                print(f"   Current rate: 1 {exchange_info['1. From_Currency Code']} = {exchange_info['5. Exchange Rate']} {exchange_info['3. To_Currency Code']}")
                print(f"   Last refreshed: {exchange_info['6. Last Refreshed']} {exchange_info['7. Time Zone']}")
                return True
            else:
                print(f"❌ Unexpected response format: {data}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Connection failed: {e}")
            return False
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON response: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False

    def get_daily_forex_data(self, limit=10):
        """
        Retrieve daily forex data for THB to EUR
        Returns up to 'limit' number of data points
        """
        print(f"\n📊 Fetching daily forex data ({self.from_symbol} to {self.to_symbol})...")
        
        try:
            params = {
                'function': 'FX_DAILY',
                'from_symbol': self.from_symbol,
                'to_symbol': self.to_symbol,
                'apikey': self.api_key,
                'outputsize': 'compact'  # Get last 100 data points
            }
            
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if "Error Message" in data:
                print(f"❌ API Error: {data['Error Message']}")
                return []
            elif "Note" in data:
                print(f"⚠️  API Limit Notice: {data['Note']}")
                return []
            elif "Time Series FX (Daily)" not in data:
                print(f"❌ Expected data not found in response: {list(data.keys())}")
                return []
            
            # Extract time series data
            time_series = data["Time Series FX (Daily)"]
            metadata = data.get("Meta Data", {})
            
            print("✅ Daily forex data retrieved successfully!")
            print(f"   Information: {metadata.get('1. Information', 'N/A')}")
            print(f"   Currency pair: {metadata.get('2. From Symbol', 'N/A')} to {metadata.get('3. To Symbol', 'N/A')}")
            print(f"   Last refreshed: {metadata.get('4. Last Refreshed', 'N/A')}")
            print(f"   Time zone: {metadata.get('5. Time Zone', 'N/A')}")
            
            # Convert to list and limit results
            forex_data = []
            for date_str, daily_data in list(time_series.items())[:limit]:
                forex_record = {
                    'date': date_str,
                    'from_currency': self.from_symbol,
                    'to_currency': self.to_symbol,
                    'open': float(daily_data['1. open']),
                    'high': float(daily_data['2. high']),
                    'low': float(daily_data['3. low']),
                    'close': float(daily_data['4. close']),
                    'timestamp': datetime.strptime(date_str, '%Y-%m-%d').isoformat(),
                }
                forex_data.append(forex_record)
            
            return forex_data
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON response: {e}")
            return []
        except KeyError as e:
            print(f"❌ Missing expected field in response: {e}")
            return []
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return []

    def print_sample_data(self, data):
        """Print sample forex data in a formatted way"""
        if not data:
            print("❌ No data to display")
            return
            
        print(f"\n📋 Sample Forex Data ({len(data)} records):")
        print("=" * 80)
        
        for i, record in enumerate(data, 1):
            print(f"\n📅 Record {i}: {record['date']}")
            print(f"   Currency Pair: {record['from_currency']} → {record['to_currency']}")
            print(f"   Open:  {record['open']:.6f}")
            print(f"   High:  {record['high']:.6f}")
            print(f"   Low:   {record['low']:.6f}")
            print(f"   Close: {record['close']:.6f}")
            print(f"   Timestamp: {record['timestamp']}")
            
        print("\n" + "=" * 80)
        
    def get_api_status_info(self):
        """Get information about API usage and limits"""
        print("\n📈 API Information:")
        print("   - Alpha Vantage provides free tier with 25 requests per day")
        print("   - Premium plans available for higher request limits")
        print("   - Forex data includes daily, weekly, monthly, and intraday options")
        print(f"   - Testing pair: {self.from_symbol} (Thai Baht) to {self.to_symbol} (Euro)")


def main():
    """Main function to run the Alpha Vantage connection test"""
    print("🚀 Alpha Vantage Forex API Connection Test")
    print("=" * 50)
    
    # Get API key from environment
    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    if not api_key:
        print("❌ Error: ALPHAVANTAGE_API_KEY environment variable not set")
        print("   Please set your Alpha Vantage API key in the environment variables")
        print("   You can get a free API key from: https://www.alphavantage.co/support/#api-key")
        return
    
    # Mask API key in logs (show only first 8 characters)
    masked_key = api_key[:8] + "..." if len(api_key) > 8 else "***"
    print(f"🔑 Using API Key: {masked_key}")
    
    # Initialize connector
    connector = AlphaVantageForexConnector(api_key)
    
    # Show API information
    connector.get_api_status_info()
    
    # Test connection
    if not connector.test_connection():
        print("\n❌ Connection test failed. Please check your API key and internet connection.")
        return
    
    # Add a small delay to respect API rate limits
    print("\n⏳ Waiting 2 seconds to respect API rate limits...")
    time.sleep(2)
    
    # Get daily forex data (10 samples)
    forex_data = connector.get_daily_forex_data(limit=10)
    
    if forex_data:
        # Print sample data
        connector.print_sample_data(forex_data)
        
        # Show data structure for future Kafka integration
        print("\n🔧 Data Structure Analysis (for future Kafka integration):")
        print("   Sample record structure:")
        if forex_data:
            sample_record = forex_data[0]
            print(f"   {json.dumps(sample_record, indent=6)}")
        
        print(f"\n✅ Successfully retrieved {len(forex_data)} forex data records!")
        print("   Connection test completed successfully.")
        print("   Ready for Kafka integration in next phase.")
        
    else:
        print("\n❌ Failed to retrieve forex data")
        print("   Please check your API key and try again")

    print("\n🏁 Connection test completed.")


if __name__ == "__main__":
    main()