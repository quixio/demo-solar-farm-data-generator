import os
import requests
import json
import time
import logging
from datetime import datetime
from quixstreams import Application
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlphaVantageSource:
    def __init__(self):
        self.api_key = os.environ.get('ALPHAVANTAGE_API_KEY', 'demo')
        self.api_url = os.environ.get('ALPHAVANTAGE_API_URL', 'https://www.alphavantage.co')
        self.from_currency = os.environ.get('FROM_CURRENCY', 'USD')
        self.to_currency = os.environ.get('TO_CURRENCY', 'EUR')
        self.poll_interval = int(os.environ.get('POLL_INTERVAL', '60'))
        self.max_messages = int(os.environ.get('MAX_MESSAGES', '100'))
        self.message_count = 0
        
    def get_exchange_rate(self) -> Optional[Dict[str, Any]]:
        """Fetch current exchange rate from Alpha Vantage API"""
        try:
            url = f"{self.api_url}/query"
            params = {
                'function': 'CURRENCY_EXCHANGE_RATE',
                'from_currency': self.from_currency,
                'to_currency': self.to_currency,
                'apikey': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API error messages
            if 'Error Message' in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
                
            if 'Note' in data:
                logger.warning(f"API Note: {data['Note']}")
                return None
                
            if 'Realtime Currency Exchange Rate' not in data:
                logger.error(f"Unexpected API response format: {data}")
                return None
                
            exchange_data = data['Realtime Currency Exchange Rate']
            
            # Transform to our schema format
            message = {
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'value': float(exchange_data.get('5. Exchange Rate', 0.0)),
                'metadata': {
                    'from_currency': exchange_data.get('1. From_Currency Code'),
                    'to_currency': exchange_data.get('3. To_Currency Code'),
                    'last_refreshed': exchange_data.get('6. Last Refreshed'),
                    'timezone': exchange_data.get('7. Time Zone'),
                    'bid_price': float(exchange_data.get('8. Bid Price', 0.0)),
                    'ask_price': float(exchange_data.get('9. Ask Price', 0.0)),
                    'source': 'alphavantage'
                }
            }
            
            return message
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            return None
        except (ValueError, KeyError) as e:
            logger.error(f"Data parsing error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def get_forex_data(self) -> Optional[Dict[str, Any]]:
        """Fetch intraday forex data as alternative data source"""
        try:
            url = f"{self.api_url}/query"
            params = {
                'function': 'FX_INTRADAY',
                'from_symbol': self.from_currency,
                'to_symbol': self.to_currency,
                'interval': '5min',
                'apikey': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Error Message' in data or 'Note' in data:
                return None
                
            time_series_key = f'Time Series FX (5min)'
            if time_series_key not in data:
                return None
                
            time_series = data[time_series_key]
            
            # Get the most recent data point
            latest_time = max(time_series.keys())
            latest_data = time_series[latest_time]
            
            message = {
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'value': float(latest_data.get('4. close', 0.0)),
                'metadata': {
                    'from_currency': self.from_currency,
                    'to_currency': self.to_currency,
                    'data_time': latest_time,
                    'open': float(latest_data.get('1. open', 0.0)),
                    'high': float(latest_data.get('2. high', 0.0)),
                    'low': float(latest_data.get('3. low', 0.0)),
                    'close': float(latest_data.get('4. close', 0.0)),
                    'source': 'alphavantage_intraday'
                }
            }
            
            return message
            
        except Exception as e:
            logger.error(f"Error fetching forex data: {e}")
            return None

def main():
    # Initialize Quix Streams application
    app = Application(
        broker_address=os.environ.get('KAFKA_BROKER_ADDRESS', 'localhost:9092'),
        consumer_group=os.environ.get('CONSUMER_GROUP', 'alphavantage-source'),
        auto_offset_reset="latest"
    )
    
    # Create output topic
    topic = app.topic(
        name=os.environ.get('OUTPUT_TOPIC', 'special-data'),
        value_serializer='json'
    )
    
    # Initialize Alpha Vantage source
    source = AlphaVantageSource()
    
    logger.info(f"Starting Alpha Vantage source application")
    logger.info(f"Output topic: {topic.name}")
    logger.info(f"Poll interval: {source.poll_interval} seconds")
    logger.info(f"Max messages: {source.max_messages}")
    
    try:
        with app.get_producer() as producer:
            while source.message_count < source.max_messages:
                try:
                    # Try to get exchange rate first
                    message = source.get_exchange_rate()
                    
                    # If exchange rate fails, try forex data
                    if message is None:
                        logger.info("Exchange rate failed, trying forex data...")
                        time.sleep(2)  # Brief delay between API calls
                        message = source.get_forex_data()
                    
                    if message is not None:
                        # Produce message to Kafka topic
                        producer.produce(
                            topic=topic.name,
                            key=f"{message['metadata']['from_currency']}-{message['metadata']['to_currency']}",
                            value=message
                        )
                        
                        source.message_count += 1
                        logger.info(f"Sent message {source.message_count}: {message['metadata']['from_currency']}/{message['metadata']['to_currency']} = {message['value']}")
                        
                    else:
                        logger.warning("No data received from Alpha Vantage API")
                    
                    # Wait before next poll
                    time.sleep(source.poll_interval)
                    
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, shutting down...")
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    time.sleep(5)  # Brief pause before retrying
                    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    
    logger.info(f"Application completed. Sent {source.message_count} messages.")

if __name__ == "__main__":
    main()