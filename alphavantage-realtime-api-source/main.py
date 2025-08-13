import os
import json
import time
import logging
import requests
from datetime import datetime
from typing import Optional, Dict, Any
from quixstreams import Application
from quixstreams.sources.base import Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlphaVantageSource(Source):
    def __init__(
        self,
        api_key: str,
        api_url: str,
        symbols: list = None,
        poll_interval: int = 12,
        name: str = "alphavantage-source"
    ):
        super().__init__(name=name)
        self.api_key = api_key
        self.api_url = api_url
        self.symbols = symbols or ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'WMT']
        self.poll_interval = poll_interval
        self.message_count = 0
        self.max_messages = 100

    def setup(self):
        """Test the API connection during setup."""
        endpoint = f"{self.api_url}/query"
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': 'AAPL',
            'apikey': self.api_key
        }
        try:
            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'Error Message' in data:
                raise RuntimeError(f"API Error during setup: {data['Error Message']}")
            if 'Global Quote' not in data or not data['Global Quote']:
                raise RuntimeError("API connection test returned unexpected response structure")

            logger.info("Successfully connected to Alpha Vantage API")

        except Exception as e:
            logger.error(f"Failed to connect to Alpha Vantage API: {e}")
            raise

    def run(self):
        """Main loop to fetch stock data from Alpha Vantage API"""
        symbol_index = 0

        while self.running and self.message_count < self.max_messages:
            try:
                symbol = self.symbols[symbol_index % len(self.symbols)]

                endpoint = f"{self.api_url}/query"
                params = {
                    'function': 'GLOBAL_QUOTE',
                    'symbol': symbol,
                    'apikey': self.api_key
                }

                logger.info(f"Fetching data for {symbol}...")

                response = requests.get(endpoint, params=params, timeout=10)

                if response.status_code == 200:
                    data = response.json()

                    if 'Error Message' in data:
                        logger.error(f"API Error for {symbol}: {data['Error Message']}")
                    elif 'Note' in data:
                        logger.warning("API rate limit reached, waiting 60 seconds...")
                        time.sleep(60)
                        continue
                    elif 'Global Quote' in data and data['Global Quote']:
                        quote = data['Global Quote']

                        if quote:
                            message = self.transform_to_kafka_format(quote)

                            if message:
                                serialized = self.serialize(
                                    key=message['symbol'],
                                    value=message
                                )

                                self.produce(
                                    key=serialized.key,
                                    value=serialized.value
                                )

                                self.message_count += 1
                                logger.info(f"Produced message {self.message_count}/{self.max_messages} for {symbol}")
                    else:
                        logger.warning(f"No data available for {symbol}")
                else:
                    logger.error(f"HTTP Error {response.status_code} for {symbol}")

            except requests.exceptions.Timeout:
                logger.error(f"Request timeout for symbol at index {symbol_index}")
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error: {str(e)[:100]}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)[:100]}")
                time.sleep(5)

            symbol_index += 1

            if self.running and self.message_count < self.max_messages:
                time.sleep(self.poll_interval)

        logger.info(f"Source completed. Processed {self.message_count} messages")

    def transform_to_kafka_format(self, quote: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform Alpha Vantage quote data to Kafka message format"""
        try:
            return {
                "symbol": quote.get('01. symbol', ''),
                "price": float(quote.get('05. price', 0)),
                "change": float(quote.get('09. change', 0)),
                "percentage_change": float(str(quote.get('10. change percent', '0%')).rstrip('%')),
                "volume": int(float(quote.get('06. volume', 0))),
                "latest_trading_day": quote.get('07. latest trading day', ''),
                "previous_close": float(quote.get('08. previous close', 0)),
                "open": float(quote.get('02. open', 0)),
                "high": float(quote.get('03. high', 0)),
                "low": float(quote.get('04. low', 0)),
                "timestamp": datetime.utcnow().isoformat()
            }
        except (ValueError, TypeError) as e:
            logger.error(f"Error transforming data: {str(e)[:100]}")
            return None

def main():
    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    if not api_key:
        raise ValueError("ALPHAVANTAGE_API_KEY environment variable is required")

    api_url = os.environ.get('ALPHAVANTAGE_API_URL', 'https://www.alphavantage.co')
    output_topic_name = os.environ.get('output', 'stock-data')

    app = Application(
        consumer_group='alphavantage-realtime-api-source',
        auto_offset_reset='latest'
    )

    source = AlphaVantageSource(
        api_key=api_key,
        api_url=api_url,
        name='alphavantage-source'
    )

    topic = app.topic(
        name=output_topic_name,
        value_serializer='json'
    )

    sdf = app.dataframe(topic=topic, source=source)
    sdf.print(metadata=True)

    logger.info("Starting Alpha Vantage source application")
    logger.info(f"Output topic: {output_topic_name}")
    logger.info(f"API URL: {api_url}")

    app.run()

if __name__ == "__main__":
    main()