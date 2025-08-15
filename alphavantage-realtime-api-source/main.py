import os
import time
import requests
import json
from datetime import datetime
from typing import Optional, Dict, Any
from quixstreams import Application
from quixstreams.sources.base import Source

# Load environment variables
api_key = os.environ.get('ALPHAVANTAGE_API_KEY', 'demo')
api_url = os.environ.get('ALPHAVANTAGE_API_URL', 'https://www.alphavantage.co')
output_topic = os.environ.get('output', 'stock-data')

class AlphaVantageForexSource(Source):
    """Source for Alpha Vantage Forex API data"""
    
    def __init__(self, api_key: str, api_url: str, from_currency: str = 'THB', to_currency: str = 'EUR', **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.api_url = api_url
        self.from_currency = from_currency
        self.to_currency = to_currency
        self.messages_produced = 0
        self.max_messages = 100  # Stop condition for testing
        self.poll_interval = 60  # Poll every 60 seconds to respect API limits
        
    def setup(self):
        """Test the API connection during setup"""
        try:
            # Test API connection with a simple request
            test_url = f"{self.api_url}/query"
            params = {
                'function': 'CURRENCY_EXCHANGE_RATE',
                'from_currency': self.from_currency,
                'to_currency': self.to_currency,
                'apikey': self.api_key
            }
            
            response = requests.get(test_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if 'Error Message' in data:
                raise ValueError("API returned error - check API key")
            elif 'Note' in data:
                print(f"API Note: Rate limit reached, will retry with delays")
            
            print(f"Successfully connected to Alpha Vantage API for {self.from_currency}/{self.to_currency}")
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to connect to Alpha Vantage API: {str(e)}")
            raise
        except Exception as e:
            print(f"Unexpected error during setup: {str(e)}")
            raise
    
    def fetch_exchange_rate(self) -> Optional[Dict[str, Any]]:
        """Fetch current exchange rate data"""
        try:
            url = f"{self.api_url}/query"
            params = {
                'function': 'CURRENCY_EXCHANGE_RATE',
                'from_currency': self.from_currency,
                'to_currency': self.to_currency,
                'apikey': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Realtime Currency Exchange Rate' in data:
                exchange_data = data['Realtime Currency Exchange Rate']
                
                # Transform to standardized format
                transformed_data = {
                    'type': 'exchange_data',
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'current_exchange_rate': {
                        'from': exchange_data.get('1. From_Currency Code', self.from_currency),
                        'to': exchange_data.get('3. To_Currency Code', self.to_currency),
                        'exchange_rate': float(exchange_data.get('5. Exchange Rate', 0)),
                        'last_refreshed': exchange_data.get('6. Last Refreshed', ''),
                        'time_zone': exchange_data.get('7. Time Zone', 'UTC'),
                        'bid_price': float(exchange_data.get('8. Bid Price', 0)),
                        'ask_price': float(exchange_data.get('9. Ask Price', 0))
                    }
                }
                
                return transformed_data
            
            elif 'Note' in data:
                print(f"API rate limit reached, will retry after delay")
                return None
            
            elif 'Error Message' in data:
                print(f"API error: Invalid request")
                return None
            
            else:
                print(f"Unexpected API response format")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching exchange rate: {str(e)}")
            return None
        except (KeyError, ValueError) as e:
            print(f"Error parsing API response: {str(e)}")
            return None
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            return None
    
    def fetch_daily_data(self) -> Optional[Dict[str, Any]]:
        """Fetch daily forex data"""
        try:
            url = f"{self.api_url}/query"
            params = {
                'function': 'FX_DAILY',
                'from_symbol': self.from_currency,
                'to_symbol': self.to_currency,
                'outputsize': 'compact',
                'apikey': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Time Series FX (Daily)' in data:
                time_series = data['Time Series FX (Daily)']
                
                daily_data = []
                for date_str, ohlc_data in sorted(time_series.items(), reverse=True)[:10]:
                    daily_data.append({
                        'date': date_str,
                        'open': float(ohlc_data.get('1. open', 0)),
                        'high': float(ohlc_data.get('2. high', 0)),
                        'low': float(ohlc_data.get('3. low', 0)),
                        'close': float(ohlc_data.get('4. close', 0))
                    })
                
                return daily_data
            
            return None
                
        except Exception as e:
            print(f"Error fetching daily data: {str(e)}")
            return None
    
    def run(self):
        """Main run loop for the source"""
        print(f"Starting Alpha Vantage Forex source for {self.from_currency}/{self.to_currency}")
        
        while self.running and self.messages_produced < self.max_messages:
            try:
                # Fetch current exchange rate
                exchange_data = self.fetch_exchange_rate()
                
                if exchange_data:
                    # Optionally fetch daily data (less frequently to respect rate limits)
                    if self.messages_produced % 5 == 0:  # Every 5th message
                        time.sleep(1)  # Small delay between API calls
                        daily_data = self.fetch_daily_data()
                        if daily_data:
                            exchange_data['daily_data'] = daily_data
                    
                    # Create message key based on currency pair
                    message_key = f"{self.from_currency}_{self.to_currency}"
                    
                    # Serialize the message
                    msg = self.serialize(
                        key=message_key,
                        value=exchange_data
                    )
                    
                    # Produce to Kafka
                    self.produce(
                        key=msg.key,
                        value=msg.value
                    )
                    
                    self.messages_produced += 1
                    print(f"Produced message {self.messages_produced}/{self.max_messages} for {message_key}")
                    
                    # Log sample data every 10 messages
                    if self.messages_produced % 10 == 0:
                        rate = exchange_data['current_exchange_rate']['exchange_rate']
                        print(f"Current {self.from_currency}/{self.to_currency} rate: {rate}")
                
                # Wait before next poll
                if self.running and self.messages_produced < self.max_messages:
                    time.sleep(self.poll_interval)
                    
            except Exception as e:
                print(f"Error in run loop: {str(e)}")
                time.sleep(5)  # Wait before retrying
        
        print(f"Source stopped. Total messages produced: {self.messages_produced}")

def main():
    # Initialize Quix application
    app = Application()
    
    # Create the Alpha Vantage source
    source = AlphaVantageForexSource(
        api_key=api_key,
        api_url=api_url,
        from_currency='THB',  # Thai Baht
        to_currency='EUR',    # Euro
        name='alphavantage-forex-source'
    )
    
    # Create output topic
    topic = app.topic(output_topic)
    
    # Create streaming dataframe with the source
    sdf = app.dataframe(topic=topic, source=source)
    
    # Print messages to console for monitoring
    sdf.print(metadata=True)
    
    # Run the application
    print("=" * 60)
    print("Starting Alpha Vantage Forex Source Application")
    print(f"Currency Pair: THB/EUR")
    print(f"Output Topic: {output_topic}")
    print(f"API URL: {api_url}")
    print("=" * 60)
    
    app.run()

if __name__ == "__main__":
    main()