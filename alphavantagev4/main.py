# Import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from quixstreams import Application

import os
import requests
import time
from datetime import datetime
from typing import Dict, Any

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class ForexDataProducer:
    """
    A Forex data producer that reads forex exchange rates from Alpha Vantage API
    and produces them to a Kafka topic.
    
    Retrieves EUR to THB exchange rates and produces them to a Kafka topic.
    """

    def __init__(self, app: Application, api_key: str, from_currency: str = "EUR", to_currency: str = "THB", poll_interval: int = 300):
        """
        Initialize the Forex data producer.
        
        Args:
            app: QuixStreams Application instance
            api_key: Alpha Vantage API key
            from_currency: Source currency code (default: EUR)
            to_currency: Target currency code (default: THB) 
            poll_interval: Time between API calls in seconds (default: 300)
        """
        self.app = app
        self.api_key = api_key
        self.from_currency = from_currency
        self.to_currency = to_currency
        self.poll_interval = poll_interval
        self.base_url = "https://www.alphavantage.co/query"
        self.messages_sent = 0
        self.max_messages = 100  # Limit for testing
        self.running = True
        
        print(f"🔧 Forex Producer Configuration:")
        print(f"   From Currency: {self.from_currency}")
        print(f"   To Currency: {self.to_currency}")
        print(f"   Poll Interval: {self.poll_interval} seconds")
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
            print(f"📡 Fetching exchange rate: {self.from_currency} -> {self.to_currency}")
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            print(f"🔍 Raw API response structure: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            
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

    def transform_to_kafka_message(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform API response data into Kafka message format based on schema.
        
        Args:
            api_data: Raw data from Alpha Vantage API
            
        Returns:
            Dict formatted for Kafka message
        """
        try:
            if "Realtime Currency Exchange Rate" in api_data:
                rate_info = api_data["Realtime Currency Exchange Rate"]
                
                # Transform to the schema format identified in documentation
                kafka_message = {
                    "eventType": "forexDataRetrieval",
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "fromCurrency": rate_info.get("1. From_Currency Code", self.from_currency),
                    "toCurrency": rate_info.get("3. To_Currency Code", self.to_currency),
                    "exchangeRate": float(rate_info.get("5. Exchange Rate", 0)),
                    "lastRefreshed": rate_info.get("6. Last Refreshed", "") + "Z",  # Add Z for UTC
                    "timeZone": rate_info.get("7. Time Zone", "UTC"),
                    "fromCurrencyName": rate_info.get("2. From_Currency Name", ""),
                    "toCurrencyName": rate_info.get("4. To_Currency Name", ""),
                    "source": "AlphaVantage"
                }
                
                print(f"💱 Transformed message: {kafka_message['fromCurrency']} -> {kafka_message['toCurrency']}: {kafka_message['exchangeRate']}")
                return kafka_message
            else:
                print(f"❌ Unexpected API response structure: {list(api_data.keys()) if isinstance(api_data, dict) else type(api_data)}")
                return None
                
        except Exception as e:
            print(f"❌ Error transforming data: {e}")
            return None

    def run(self, output_topic):
        """
        Main execution method for the Producer.
        
        Continuously fetches forex data and produces to Kafka topic.
        """
        print("🚀 Starting Forex data producer...")
        
        with self.app.get_producer() as producer:
            while self.running and self.messages_sent < self.max_messages:
                try:
                    # Get current exchange rate
                    api_data = self.get_current_exchange_rate()
                    
                    # Transform to Kafka message format
                    kafka_message = self.transform_to_kafka_message(api_data)
                    
                    if kafka_message:
                        # Create a key from the currency pair
                        message_key = f"{kafka_message['fromCurrency']}-{kafka_message['toCurrency']}"
                        
                        # Serialize and produce to Kafka
                        serialized_message = output_topic.serialize(
                            key=message_key,
                            value=kafka_message
                        )
                        producer.produce(
                            topic=output_topic.name,
                            key=serialized_message.key,
                            value=serialized_message.value
                        )
                        
                        self.messages_sent += 1
                        print(f"✅ Produced forex message {self.messages_sent}/{self.max_messages} - Rate: {kafka_message['exchangeRate']}")
                    else:
                        print("⚠️  Skipped message due to transformation error")
                    
                    # Wait before next API call to respect rate limits
                    if self.running and self.messages_sent < self.max_messages:
                        print(f"⏳ Waiting {self.poll_interval} seconds before next fetch...")
                        time.sleep(self.poll_interval)
                        
                except Exception as e:
                    print(f"❌ Error in forex producer: {e}")
                    print(f"⏳ Waiting {self.poll_interval} seconds before retry...")
                    if self.running:
                        time.sleep(self.poll_interval)
        
        print(f"🏁 Forex producer finished. Produced {self.messages_sent} messages.")


def main():
    """Main function to set up and run the Forex data producer."""
    try:
        print("🚀 Starting Forex Data Producer Application...")
        print()
        
        # Get environment variables
        api_key = os.environ.get("API_KEY")
        from_currency = os.environ.get("from_currency", "EUR")
        to_currency = os.environ.get("to_currency", "THB")
        output_topic_name = os.environ.get("output", "stock-data")
        
        # Validate required environment variables
        if not api_key:
            raise ValueError("API_KEY environment variable is required")
        
        # Setup Quix Streams Application
        app = Application(consumer_group="forex_data_producer", auto_create_topics=True)
        
        # Create output topic
        output_topic = app.topic(name=output_topic_name)
        
        # Create the forex data producer
        forex_producer = ForexDataProducer(
            app=app,
            api_key=api_key,
            from_currency=from_currency,
            to_currency=to_currency,
            poll_interval=300  # 5 minutes between API calls
        )
        
        print(f"📤 Output topic: {output_topic_name}")
        print("🎯 Starting producer - press Ctrl+C to stop")
        print()
        
        # Run the producer
        forex_producer.run(output_topic)
        
    except KeyboardInterrupt:
        print("\n⚠️  Application stopped by user")
    except Exception as e:
        print(f"❌ Application error: {e}")
        raise


if __name__ == "__main__":
    main()