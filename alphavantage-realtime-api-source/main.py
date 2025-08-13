import os
import time
import json
import requests
from datetime import datetime
from typing import Optional, Dict, Any
from quixstreams import Application
from quixstreams.sources.base import Source


class AlphaVantageSource(Source):
    def __init__(
        self,
        api_key: str,
        api_url: str,
        symbols: list = None,
        interval: str = "5min",
        rate_limit_delay: int = 12,
        name: str = "alphavantage-source"
    ):
        super().__init__(name=name)
        self.api_key = api_key
        self.api_url = api_url
        self.symbols = symbols or ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA',
                                   'META', 'NVDA', 'JPM', 'V', 'WMT']
        self.interval = interval
        self.rate_limit_delay = rate_limit_delay
        self.messages_produced = 0
        self.max_messages = 100

    def setup(self):
        """Test the API connection during setup"""
        try:
            test_endpoint = f"{self.api_url}/query"
            test_params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': 'AAPL',
                'interval': self.interval,
                'apikey': self.api_key,
                'outputsize': 'compact'
            }
            response = requests.get(test_endpoint, params=test_params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")

            if 'Note' in data:
                self.logger.warning(f"API Note: {data['Note']}")

            self.logger.info("Successfully connected to Alpha Vantage API")

        except Exception as e:
            self.logger.error(f"Failed to connect to Alpha Vantage API: {str(e)}")
            raise

    def fetch_stock_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch stock data for a given symbol"""
        try:
            endpoint = f"{self.api_url}/query"
            params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': symbol,
                'interval': self.interval,
                'apikey': self.api_key,
                'outputsize': 'compact'
            }

            response = requests.get(endpoint, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if 'Error Message' in data:
                self.logger.error(f"API Error for {symbol}: {data['Error Message']}")
                return None

            if 'Note' in data:
                self.logger.warning(f"API rate limit reached: {data['Note']}")
                time.sleep(self.rate_limit_delay * 2)
                return None

            series_key = f"Time Series ({self.interval})"
            if series_key not in data:
                self.logger.warning(f"No time series data for {symbol} (interval={self.interval})")
                return None

            time_series = data[series_key]
            meta_data = data.get('Meta Data', {})

            timestamps = list(time_series.keys())
            if not timestamps:
                return None

            latest_timestamp = max(timestamps)
            latest_data = time_series[latest_timestamp]

            return {
                'item_number': self.messages_produced + 1,
                'symbol': symbol,
                'timestamp': latest_timestamp,
                'last_refreshed': meta_data.get('3. Last Refreshed', latest_timestamp),
                'time_zone': meta_data.get('6. Time Zone', 'US/Eastern'),
                'open': float(latest_data.get('1. open', 0)),
                'high': float(latest_data.get('2. high', 0)),
                'low': float(latest_data.get('3. low', 0)),
                'close': float(latest_data.get('4. close', 0)),
                'volume': int(latest_data.get('5. volume', 0))
            }

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for {symbol}: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error for {symbol}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error for {symbol}: {str(e)}")
            return None

    def run(self):
        """Main run loop for the source"""
        self.logger.info(f"Starting Alpha Vantage source with symbols: {self.symbols}")

        symbol_index = 0

        while self.running and self.messages_produced < self.max_messages:
            try:
                symbol = self.symbols[symbol_index % len(self.symbols)]

                stock_data = self.fetch_stock_data(symbol)

                if stock_data:
                    msg = self.serialize(
                        key=f"{symbol}_{stock_data['timestamp']}",
                        value=stock_data
                    )

                    self.produce(
                        key=msg.key,
                        value=msg.value
                    )

                    self.messages_produced += 1
                    self.logger.info(f"Produced message {self.messages_produced}/{self.max_messages} for {symbol}")

                    if self.messages_produced >= self.max_messages:
                        self.logger.info(f"Reached maximum message limit of {self.max_messages}")
                        break

                symbol_index += 1

                if self.running and self.messages_produced < self.max_messages:
                    time.sleep(self.rate_limit_delay)

            except Exception as e:
                self.logger.error(f"Error in run loop: {str(e)}")
                time.sleep(self.rate_limit_delay)
                continue

        self.logger.info("Alpha Vantage source stopped")


def main():
    app = Application()

    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    if not api_key:
        raise ValueError("ALPHAVANTAGE_API_KEY environment variable is not set")

    api_url = os.environ.get('ALPHAVANTAGE_API_URL', 'https://www.alphavantage.co')

    source = AlphaVantageSource(
        api_key=api_key,
        api_url=api_url,
        symbols=['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA',
                 'META', 'NVDA', 'JPM', 'V', 'WMT'],
        interval="5min",
        rate_limit_delay=12,
        name="alphavantage-realtime-api-source"
    )

    output_topic_name = os.environ.get('output', 'stock-data')
    topic = app.topic(output_topic_name)

    sdf = app.dataframe(topic=topic, source=source)
    sdf.print(metadata=True)

    app.run()


if __name__ == "__main__":
    main()