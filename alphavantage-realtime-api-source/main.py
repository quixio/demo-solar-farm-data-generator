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

RATE_LIMIT_SLEEP = 65  # seconds (free tier can be spiky; be generous)

class AlphaVantageSource(Source):
    def __init__(
        self,
        api_key: str,
        api_url: str,
        symbols: list | None = None,
        poll_interval: int = 12,
        name: str = "alphavantage-source",
        max_messages: int = 100,
    ):
        super().__init__(name=name)
        self.api_key = api_key
        self.api_url = api_url.rstrip("/")
        self.symbols = symbols or ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'WMT']
        self.poll_interval = poll_interval
        self.message_count = 0
        self.max_messages = max_messages
        self._http = requests.Session()

    def _request_quote(self, symbol: str) -> Dict[str, Any]:
        endpoint = f"{self.api_url}/query"
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key,
            # "datatype": "json"  # implicit; leave commented for clarity
        }
        resp = self._http.get(endpoint, params=params, timeout=15)
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            # Alpha Vantage sometimes responds with HTML when throttled/mis-keyed
            text = resp.text[:200].replace("\n", " ")
            raise RuntimeError(f"Non‑JSON response from API: {text}")

    def setup(self):
        """Best‑effort connectivity check without killing the source on rate‑limit noise."""
        test_symbol = (self.symbols[0] if self.symbols else "AAPL")
        try:
            data = self._request_quote(test_symbol)

            if "Error Message" in data:
                raise RuntimeError(f"API error: {data['Error Message']}")

            if "Note" in data:
                logger.warning(f"Setup: rate limited by API. Proceeding anyway. Note: {data['Note'][:120]}...")
                return

            if "Information" in data:
                logger.warning(f"Setup: informational message from API. Proceeding. Info: {data['Information'][:120]}...")
                return

            if not data.get("Global Quote"):
                # Don’t kill the process; Alpha Vantage is flaky. Log and continue.
                logger.warning("Setup: API responded without 'Global Quote'. Proceeding anyway.")
                return

            logger.info("Successfully connected to Alpha Vantage API")

        except requests.HTTPError as e:
            raise RuntimeError(f"HTTP error during setup: {e.response.status_code} {e.response.reason}") from e
        except Exception as e:
            # Only hard‑fail for truly unexpected errors
            raise RuntimeError(f"Failed to connect to Alpha Vantage API: {e}") from e

    def run(self):
        """Main loop to fetch stock data from Alpha Vantage API."""
        symbol_index = 0

        while self.running and self.message_count < self.max_messages:
            symbol = self.symbols[symbol_index % len(self.symbols)]
            try:
                logger.info(f"Fetching data for {symbol}...")
                data = self._request_quote(symbol)

                if "Error Message" in data:
                    logger.error(f"API Error for {symbol}: {data['Error Message']}")
                elif "Note" in data:
                    logger.warning(f"Rate limit reached. Backing off {RATE_LIMIT_SLEEP}s…")
                    time.sleep(RATE_LIMIT_SLEEP)
                    continue
                elif "Information" in data:
                    logger.warning(f"API sent informational throttle message. Backing off {RATE_LIMIT_SLEEP}s…")
                    time.sleep(RATE_LIMIT_SLEEP)
                    continue
                elif data.get("Global Quote"):
                    quote = data["Global Quote"] or {}
                    if quote:
                        # Ensure symbol is present even if AV omits it
                        quote.setdefault("01. symbol", symbol)
                        message = self.transform_to_kafka_format(quote)
                        if message:
                            serialized = self.serialize(
                                key=message["symbol"],
                                value=message
                            )
                            self.produce(
                                key=serialized.key,
                                value=serialized.value
                            )
                            self.message_count += 1
                            logger.info(f"Produced message {self.message_count}/{self.max_messages} for {symbol}")
                    else:
                        logger.warning(f"No quote payload for {symbol}")
                else:
                    logger.warning(f"No data available for {symbol}")

            except requests.exceptions.Timeout:
                logger.error(f"Request timeout for {symbol}")
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error: {str(e)[:200]}")
                time.sleep(5)
            except requests.HTTPError as e:
                logger.error(f"HTTP error for {symbol}: {e.response.status_code} {e.response.reason}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error for {symbol}: {str(e)[:200]}")
                time.sleep(5)

            symbol_index += 1

            if self.running and self.message_count < self.max_messages:
                time.sleep(self.poll_interval)

        logger.info(f"Source completed. Processed {self.message_count} messages")

    @staticmethod
    def _to_float(v, default=0.0) -> float:
        # AV returns strings; sometimes empty. Strip % and commas.
        if v is None:
            return default
        s = str(v).replace(",", "").strip().rstrip("%")
        if s == "":
            return default
        try:
            return float(s)
        except ValueError:
            return default

    @staticmethod
    def _to_int(v, default=0) -> int:
        try:
            return int(float(str(v).replace(",", "").strip()))
        except ValueError:
            return default

    def transform_to_kafka_format(self, quote: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform Alpha Vantage quote data to Kafka message format."""
        try:
            return {
                "symbol": quote.get("01. symbol", ""),
                "price": self._to_float(quote.get("05. price")),
                "change": self._to_float(quote.get("09. change")),
                "percentage_change": self._to_float(quote.get("10. change percent")),
                "volume": self._to_int(quote.get("06. volume")),
                "latest_trading_day": quote.get("07. latest trading day", ""),
                "previous_close": self._to_float(quote.get("08. previous close")),
                "open": self._to_float(quote.get("02. open")),
                "high": self._to_float(quote.get("03. high")),
                "low": self._to_float(quote.get("04. low")),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
        except Exception as e:
            logger.error(f"Error transforming data: {str(e)[:200]}")
            return None


def main():
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise ValueError("ALPHAVANTAGE_API_KEY environment variable is required")

    api_url = os.environ.get("ALPHAVANTAGE_API_URL", "https://www.alphavantage.co")
    output_topic_name = os.environ.get("output", "stock-data")

    app = Application(
        consumer_group="alphavantage-realtime-api-source",
        auto_offset_reset="latest",
    )

    source = AlphaVantageSource(
        api_key=api_key,
        api_url=api_url,
        name="alphavantage-source",
    )

    topic = app.topic(
        name=output_topic_name,
        value_serializer="json",
    )

    sdf = app.dataframe(topic=topic, source=source)
    sdf.print(metadata=True)

    logger.info("Starting Alpha Vantage source application")
    logger.info(f"Output topic: {output_topic_name}")
    logger.info(f"API URL: {api_url}")

    app.run()


if __name__ == "__main__":
    main()
