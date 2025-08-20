import os
import requests
import time
from datetime import datetime
import json
import logging

from quixstreams import Application
from quixstreams.sources import Source

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlphaVantageForexSource(Source):
    """
    A Quix Streams Source for reading forex data from Alpha Vantage API.
    
    This source retrieves Thai Baht (THB) to Euro (EUR) exchange rates from
    the Alpha Vantage API and produces them to a Kafka topic.
    """
    
    def __init__(self, api_key, name="alphavantage-forex-source", shutdown_timeout=10):
        super().__init__(name=name, shutdown_timeout=shutdown_timeout)
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.from_symbol = "THB"  # Thai Baht
        self.to_symbol = "EUR"    # Euro
        self.session = requests.Session()
        # Set timeout and retry parameters
        self.session.timeout = 30
        self.records_produced = 0
        self.max_records = 100  # Limit for testing
        
    def setup(self):
        """Test connection to Alpha Vantage API before starting"""
        logger.info("🔗 Setting up connection to Alpha Vantage API...")
        
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
                raise Exception(f"API Error: {data['Error Message']}")
            elif "Note" in data:
                raise Exception(f"API Limit Notice: {data['Note']}")
            elif "Realtime Currency Exchange Rate" in data:
                logger.info("✅ Connection to Alpha Vantage API successful!")
                exchange_info = data["Realtime Currency Exchange Rate"]
                logger.info(f"   Current rate: 1 {exchange_info['1. From_Currency Code']} = {exchange_info['5. Exchange Rate']} {exchange_info['3. To_Currency Code']}")
                logger.info(f"   Last refreshed: {exchange_info['6. Last Refreshed']} {exchange_info['7. Time Zone']}")
                return
            else:
                raise Exception(f"Unexpected response format: {list(data.keys())}")
                
        except requests.exceptions.RequestException as e:
            raise Exception(f"Connection failed: {e}")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON response: {e}")
        except Exception as e:
            raise Exception(f"Setup failed: {e}")

    def run(self):
        """
        Main loop for the source. Fetches forex data and produces to Kafka.
        """
        logger.info(f"🚀 Starting Alpha Vantage Forex Source ({self.from_symbol} to {self.to_symbol})")
        
        try:
            while self.running and self.records_produced < self.max_records:
                logger.info(f"📊 Fetching daily forex data ({self.from_symbol} to {self.to_symbol})...")
                
                # Fetch daily forex data
                forex_data = self._fetch_daily_forex_data()
                
                if not forex_data:
                    logger.warning("No forex data retrieved, waiting before retry...")
                    time.sleep(60)  # Wait 1 minute before retrying
                    continue
                
                # Process and produce each record
                for record in forex_data:
                    if not self.running or self.records_produced >= self.max_records:
                        break
                        
                    # Create a message key using currency pair and date
                    message_key = f"{record['from_currency']}_{record['to_currency']}_{record['date']}"
                    
                    # Add some debugging info
                    logger.info(f"📤 Producing record: {message_key}")
                    print(f"Raw record structure: {record}")
                    
                    # Serialize and produce the message
                    serialized_msg = self.serialize(key=message_key, value=record)
                    self.produce(
                        key=serialized_msg.key,
                        value=serialized_msg.value
                    )
                    
                    self.records_produced += 1
                    logger.info(f"✅ Produced record {self.records_produced}: {message_key}")
                    
                    # Small delay between messages
                    time.sleep(0.1)
                
                # Wait before next API call to respect rate limits
                if self.running and self.records_produced < self.max_records:
                    logger.info("⏳ Waiting 5 minutes before next API call...")
                    time.sleep(300)  # Wait 5 minutes between batches
                    
        except Exception as e:
            logger.error(f"❌ Error in run loop: {e}")
            raise
        
        logger.info(f"🏁 Alpha Vantage Forex Source completed. Produced {self.records_produced} records.")

    def _fetch_daily_forex_data(self, limit=20):
        """
        Retrieve daily forex data for THB to EUR
        Returns up to 'limit' number of data points
        """
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
                logger.error(f"❌ API Error: {data['Error Message']}")
                return []
            elif "Note" in data:
                logger.warning(f"⚠️  API Limit Notice: {data['Note']}")
                return []
            elif "Time Series FX (Daily)" not in data:
                logger.error(f"❌ Expected data not found in response: {list(data.keys())}")
                return []
            
            # Extract time series data
            time_series = data["Time Series FX (Daily)"]
            metadata = data.get("Meta Data", {})
            
            logger.info("✅ Daily forex data retrieved successfully!")
            logger.info(f"   Information: {metadata.get('1. Information', 'N/A')}")
            logger.info(f"   Currency pair: {metadata.get('2. From Symbol', 'N/A')} to {metadata.get('3. To Symbol', 'N/A')}")
            logger.info(f"   Last refreshed: {metadata.get('4. Last Refreshed', 'N/A')}")
            logger.info(f"   Time zone: {metadata.get('5. Time Zone', 'N/A')}")
            
            # Convert to list and limit results
            forex_data = []
            for date_str, daily_data in list(time_series.items())[:limit]:
                # Create timestamp in nanoseconds (Quix format)
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                timestamp_ns = int(dt.timestamp() * 1_000_000_000)
                
                forex_record = {
                    'date': date_str,
                    'from_currency': self.from_symbol,
                    'to_currency': self.to_symbol,
                    'open': float(daily_data['1. open']),
                    'high': float(daily_data['2. high']),
                    'low': float(daily_data['3. low']),
                    'close': float(daily_data['4. close']),
                    'timestamp': timestamp_ns,
                    'symbol': f"{self.from_symbol}{self.to_symbol}",
                    'data_source': 'alphavantage',
                    'data_type': 'forex_daily'
                }
                forex_data.append(forex_record)
            
            return forex_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request failed: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON response: {e}")
            return []
        except KeyError as e:
            logger.error(f"❌ Missing expected field in response: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            return []


def main():
    """Main function to run the Alpha Vantage Forex Source with Quix Streams"""
    logger.info("🚀 Starting Alpha Vantage Forex Source Application")
    
    # Get configuration from environment variables
    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    if not api_key:
        logger.error("❌ Error: ALPHAVANTAGE_API_KEY environment variable not set")
        logger.error("   Please set your Alpha Vantage API key in the environment variables")
        logger.error("   You can get a free API key from: https://www.alphavantage.co/support/#api-key")
        return

    output_topic_name = os.environ.get('output', 'stock-data')
    
    # Mask API key in logs (show only first 8 characters)
    masked_key = api_key[:8] + "..." if len(api_key) > 8 else "***"
    logger.info(f"🔑 Using API Key: {masked_key}")
    logger.info(f"📤 Output Topic: {output_topic_name}")
    
    try:
        # Setup Quix Streams Application
        app = Application(
            consumer_group="alphavantage-forex-producer",
            auto_create_topics=True
        )
        
        # Create the source
        forex_source = AlphaVantageForexSource(api_key=api_key)
        
        # Define the output topic
        output_topic = app.topic(name=output_topic_name)
        
        # Setup StreamingDataFrame for data processing and monitoring
        sdf = app.dataframe(source=forex_source, topic=output_topic)
        
        # Print messages to console for debugging
        sdf.print(metadata=True)
        
        # Write to the output topic
        sdf.to_topic(output_topic)
        
        logger.info("🔄 Application configured successfully")
        logger.info("⚡ Starting application...")
        
        # Run the application
        app.run()
        
    except KeyboardInterrupt:
        logger.info("🛑 Application stopped by user")
    except Exception as e:
        logger.error(f"❌ Application failed: {e}")
        raise


if __name__ == "__main__":
    main()