# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sources, see https://quix.io/docs/quix-streams/connectors/sources/index.html
from quixstreams import Application
from quixstreams.sources import Source

import os
import requests
import time
import logging
from datetime import datetime
from typing import List, Dict, Any

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlphaVantageStockSource(Source):
    """
    A Quix Streams Source that fetches daily stock data from Alpha Vantage API
    for specified stock tickers and publishes to a Kafka topic.
    
    This source reads daily stock data (OHLCV) for GOOG and AAPL from Alpha Vantage
    and transforms it into structured messages for downstream processing.
    """

    def __init__(self, name: str, api_key: str, tickers: List[str] = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.api_key = api_key
        self.tickers = tickers or ["GOOG", "AAPL"]
        self.base_url = "https://www.alphavantage.co/query"
        self.message_count = 0
        self.max_messages = 100  # Limit for testing
        
        logger.info(f"Initialized AlphaVantageStockSource for tickers: {self.tickers}")

    def _fetch_daily_stock_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch daily stock data for a given symbol from Alpha Vantage API
        """
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'compact'  # Get latest 100 data points
        }
        
        try:
            logger.info(f"Fetching daily data for {symbol}")
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Debug: Print raw API response structure
            print(f"Raw API response keys for {symbol}: {list(data.keys())}")
            
            if 'Error Message' in data:
                logger.error(f"API Error for {symbol}: {data['Error Message']}")
                return None
                
            if 'Note' in data:
                logger.warning(f"API Rate limit warning for {symbol}: {data['Note']}")
                return None
                
            if 'Time Series (Daily)' not in data:
                logger.error(f"No daily data found for {symbol}. Available keys: {list(data.keys())}")
                return None
                
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching data for {symbol}: {e}")
            return None

    def _process_stock_data(self, symbol: str, api_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process the API response and convert to structured messages
        """
        if not api_response or 'Time Series (Daily)' not in api_response:
            return []
        
        time_series = api_response['Time Series (Daily)']
        metadata = api_response.get('Meta Data', {})
        
        messages = []
        for date_str, daily_data in time_series.items():
            try:
                # Convert date string to ISO format timestamp
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                timestamp = date_obj.isoformat()
                
                # Create structured message
                message = {
                    'symbol': symbol,
                    'date': date_str,
                    'timestamp': timestamp,
                    'open': float(daily_data['1. open']),
                    'high': float(daily_data['2. high']),
                    'low': float(daily_data['3. low']),
                    'close': float(daily_data['4. close']),
                    'volume': int(daily_data['5. volume']),
                    'last_refreshed': metadata.get('3. Last Refreshed', ''),
                    'time_zone': metadata.get('5. Time Zone', 'US/Eastern')
                }
                messages.append(message)
                
            except (ValueError, KeyError) as e:
                logger.error(f"Error processing data point for {symbol} on {date_str}: {e}")
                continue
        
        # Sort by date (most recent first)
        messages.sort(key=lambda x: x['date'], reverse=True)
        logger.info(f"Processed {len(messages)} data points for {symbol}")
        
        return messages

    def run(self):
        """
        Main execution loop that fetches stock data and produces messages to Kafka
        """
        logger.info("Starting AlphaVantageStockSource...")
        
        for ticker in self.tickers:
            if not self.running:
                break
                
            # Check message limit
            if self.message_count >= self.max_messages:
                logger.info(f"Reached maximum message limit ({self.max_messages}). Stopping.")
                break
            
            # Fetch data for this ticker
            api_data = self._fetch_daily_stock_data(ticker)
            if not api_data:
                logger.warning(f"Skipping {ticker} due to API error")
                continue
            
            # Process and send messages
            messages = self._process_stock_data(ticker, api_data)
            
            for message in messages:
                if not self.running:
                    break
                    
                if self.message_count >= self.max_messages:
                    logger.info(f"Reached maximum message limit ({self.max_messages}). Stopping.")
                    break
                
                try:
                    # Debug: Print message structure
                    print(f"Producing message for {ticker}: {message}")
                    
                    # Serialize and produce message
                    event_serialized = self.serialize(key=ticker, value=message)
                    self.produce(key=event_serialized.key, value=event_serialized.value)
                    
                    self.message_count += 1
                    logger.info(f"Successfully produced message {self.message_count} for {ticker} on {message['date']}")
                    
                except Exception as e:
                    logger.error(f"Failed to produce message for {ticker}: {e}")
            
            # Rate limiting: Wait between tickers to avoid API limits
            if self.running and ticker != self.tickers[-1]:  # Don't wait after last ticker
                logger.info("Waiting 12 seconds between API calls to respect rate limits...")
                time.sleep(12)  # Alpha Vantage allows 5 calls per minute for free tier
        
        logger.info(f"AlphaVantageStockSource completed. Total messages produced: {self.message_count}")


def main():
    """ Here we will set up our Application. """
    
    # Get environment variables
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        logger.error("ALPHA_VANTAGE_API_KEY environment variable is required")
        raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required")
    
    output_topic_name = os.environ.get("output", "stock-data")
    logger.info(f"Using output topic: {output_topic_name}")

    # Setup necessary objects
    app = Application(consumer_group="stock_data_producer", auto_create_topics=True)
    
    # Create Alpha Vantage stock source for GOOG and AAPL
    stock_source = AlphaVantageStockSource(
        name="alphavantage-stock-producer",
        api_key=api_key,
        tickers=["GOOG", "AAPL"]
    )
    
    output_topic = app.topic(name=output_topic_name)

    # Setup streaming dataframe with debugging
    sdf = app.dataframe(source=stock_source)
    sdf.print(metadata=True)  # This will help users see messages being produced
    
    # Process the data through SDF and output to topic
    sdf.to_topic(topic=output_topic)

    logger.info("Starting Quix Streams application...")
    # With our pipeline defined, now run the Application
    app.run()


#  Sources require execution under a conditional main
if __name__ == "__main__":
    main()