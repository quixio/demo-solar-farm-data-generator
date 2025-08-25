"""
Forex Data Connection Test

This script tests the connection to Alpha Vantage forex API to retrieve EUR to THB exchange rates.
This is a connection test only - no Kafka/Quix Streams integration yet.
"""

import os
import sys
import time
import requests
from datetime import datetime
from typing import Dict, Any, List

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class ForexConnectionTester:
    """
    Test connection to Alpha Vantage API for forex exchange rates.
    Retrieves sample EUR to THB exchange rate data.
    """
    
    def __init__(self):
        """Initialize the forex connection tester with required configuration."""
        self.api_key = os.environ.get("API_KEY")
        self.from_currency = os.environ.get("from_currency", "EUR")
        self.to_currency = os.environ.get("to_currency", "THB")
        self.base_url = "https://www.alphavantage.co/query"
        
        # Validate required environment variables
        if not self.api_key:
            raise ValueError("API_KEY environment variable is required")
            
        print(f"🔧 Configuration:")
        print(f"   From Currency: {self.from_currency}")
        print(f"   To Currency: {self.to_currency}")
        print(f"   API Key: {'*' * (len(self.api_key) - 4) + self.api_key[-4:] if len(self.api_key) > 4 else '****'}")
        print()

    def get_current_exchange_rate(self) -> Dict[str, Any]:
        """
        Retrieve current exchange rate for the currency pair.
        
        Returns:
            Dict containing exchange rate information
        """
        params = {
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_currency": self.from_currency,
            "to_currency": self.to_currency,
            "apikey": self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if "Error Message" in data:
                raise Exception(f"API Error: {data['Error Message']}")
            if "Note" in data:
                print(f"⚠️  API Note: {data['Note']}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"HTTP request failed: {e}")
        except Exception as e:
            raise Exception(f"Failed to retrieve current exchange rate: {e}")

    def get_daily_forex_data(self, output_size: str = "compact") -> Dict[str, Any]:
        """
        Retrieve daily forex time series data.
        
        Args:
            output_size: "compact" (last 100 data points) or "full" (full history)
            
        Returns:
            Dict containing daily forex time series data
        """
        params = {
            "function": "FX_DAILY",
            "from_symbol": self.from_currency,
            "to_symbol": self.to_currency,
            "outputsize": output_size,
            "apikey": self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if "Error Message" in data:
                raise Exception(f"API Error: {data['Error Message']}")
            if "Note" in data:
                print(f"⚠️  API Note: {data['Note']}")
                
            return data
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"HTTP request failed: {e}")
        except Exception as e:
            raise Exception(f"Failed to retrieve daily forex data: {e}")

    def format_current_rate(self, data: Dict[str, Any]) -> None:
        """Format and display current exchange rate information."""
        if "Realtime Currency Exchange Rate" not in data:
            print("❌ No exchange rate data found in response")
            return
            
        rate_info = data["Realtime Currency Exchange Rate"]
        
        print("💱 CURRENT EXCHANGE RATE:")
        print(f"   From: {rate_info.get('2. From_Currency Name', 'N/A')} ({rate_info.get('1. From_Currency Code', 'N/A')})")
        print(f"   To: {rate_info.get('4. To_Currency Name', 'N/A')} ({rate_info.get('3. To_Currency Code', 'N/A')})")
        print(f"   Rate: {float(rate_info.get('5. Exchange Rate', 0)):.6f}")
        print(f"   Last Refreshed: {rate_info.get('6. Last Refreshed', 'N/A')} UTC")
        print(f"   Time Zone: {rate_info.get('7. Time Zone', 'N/A')}")
        print()

    def format_daily_rates(self, data: Dict[str, Any], limit: int = 10) -> None:
        """Format and display daily forex rate samples."""
        if "Time Series (Daily)" not in data:
            print("❌ No daily time series data found in response")
            return
            
        meta_data = data.get("Meta Data", {})
        time_series = data["Time Series (Daily)"]
        
        print("📊 FOREX METADATA:")
        for key, value in meta_data.items():
            print(f"   {key}: {value}")
        print()
        
        print(f"📈 DAILY EXCHANGE RATES (Showing {min(limit, len(time_series))} most recent):")
        print(f"   {'Date':<12} {'Open':<10} {'High':<10} {'Low':<10} {'Close':<10}")
        print(f"   {'-'*12} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
        
        # Sort dates in descending order and take the first `limit` entries
        sorted_dates = sorted(time_series.keys(), reverse=True)
        
        for i, date in enumerate(sorted_dates[:limit]):
            rates = time_series[date]
            print(f"   {date:<12} {float(rates['1. open']):<10.6f} {float(rates['2. high']):<10.6f} {float(rates['3. low']):<10.6f} {float(rates['4. close']):<10.6f}")
        
        print()
        print(f"📈 Total available data points: {len(time_series)}")
        print()

    def test_connection(self) -> bool:
        """
        Test the connection to Alpha Vantage API and retrieve sample data.
        
        Returns:
            True if connection test successful, False otherwise
        """
        print("🔍 FOREX DATA CONNECTION TEST")
        print("=" * 50)
        
        try:
            # Test 1: Current exchange rate
            print("📡 Testing current exchange rate retrieval...")
            current_rate_data = self.get_current_exchange_rate()
            print("✅ Successfully retrieved current exchange rate")
            self.format_current_rate(current_rate_data)
            
            # Brief pause between API calls to respect rate limits
            time.sleep(1)
            
            # Test 2: Daily historical data
            print("📡 Testing daily historical data retrieval...")
            daily_data = self.get_daily_forex_data()
            print("✅ Successfully retrieved daily forex data")
            self.format_daily_rates(daily_data, limit=10)
            
            print("🎉 CONNECTION TEST SUCCESSFUL!")
            print("   ✅ API authentication working")
            print("   ✅ Data retrieval functioning")
            print("   ✅ Response parsing successful")
            print("   ✅ Sample data retrieved and formatted")
            print()
            print("💡 Next steps: This data structure can be used for Kafka integration")
            
            return True
            
        except Exception as e:
            print(f"❌ CONNECTION TEST FAILED: {e}")
            print()
            print("🔧 Troubleshooting tips:")
            print("   • Verify API_KEY environment variable is set correctly")
            print("   • Check Alpha Vantage account status and rate limits")
            print("   • Ensure currency codes are valid (EUR, THB)")
            print("   • Check internet connectivity")
            
            return False


def main():
    """Main function to run the forex connection test."""
    try:
        print("🚀 Starting Forex Data Connection Test...")
        print()
        
        # Initialize and run the connection test
        tester = ForexConnectionTester()
        success = tester.test_connection()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()