# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sources, see https://quix.io/docs/quix-streams/connectors/sources/index.html
from quixstreams import Application
from quixstreams.sources import Source

import os
import json
import time
from datetime import datetime
from typing import Dict, Any, List
from websocket import WebSocketApp
import websocket

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class MemoryUsageGenerator(Source):
    """
    A Quix Streams Source enables Applications to read data from something other
    than Kafka and publish it to a desired Kafka topic.

    You provide a Source to an Application, which will handle the Source's lifecycle.

    In this case, we have built a new Source that reads from a static set of
    already loaded json data representing a server's memory usage over time.

    There are numerous pre-built sources available to use out of the box; see:
    https://quix.io/docs/quix-streams/connectors/sources/index.html
    """
    
    def __init__(self):
        """Initialize the WebSocket tester with configuration from environment variables."""
        self.websocket_url = os.environ.get("WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
        self.sample_count = int(os.environ.get("SAMPLE_COUNT", "10"))
        self.connection_timeout = int(os.environ.get("CONNECTION_TIMEOUT", "30"))
        
        self.collected_transactions = []
        self.ws = None
        self.connection_successful = False
        self.start_time = None
        
    def on_open(self, ws):
        """Called when WebSocket connection is opened."""
        print(f"✅ WebSocket connection opened successfully!")
        print(f"🔗 Connected to: {self.websocket_url}")
        print(f"📊 Requesting {self.sample_count} sample transactions...")
        print("=" * 60)
        
        self.connection_successful = True
        self.start_time = time.time()
        
        # Subscribe to unconfirmed transactions
        subscribe_message = {"op": "unconfirmed_sub"}
        ws.send(json.dumps(subscribe_message))
        print("📡 Subscribed to unconfirmed Bitcoin transactions")
        
    def on_message(self, ws, message):
        """Called when a message is received from WebSocket."""
        try:
            data = json.loads(message)
            
            # Check if this is a transaction message
            if data.get("op") == "utx" and "x" in data:
                transaction = data["x"]
                self.collected_transactions.append(transaction)
                
                tx_count = len(self.collected_transactions)
                print(f"\n📦 Transaction #{tx_count} received:")
                print(f"   Hash: {transaction.get('hash', 'N/A')}")
                print(f"   Size: {transaction.get('size', 'N/A')} bytes")
                print(f"   Inputs: {transaction.get('vin_sz', 'N/A')}")
                print(f"   Outputs: {transaction.get('vout_sz', 'N/A')}")
                print(f"   Time: {datetime.fromtimestamp(transaction.get('time', 0))}")
                
                # Print first input address for reference
                if transaction.get('inputs') and len(transaction['inputs']) > 0:
                    first_input = transaction['inputs'][0]
                    if 'prev_out' in first_input and 'addr' in first_input['prev_out']:
                        print(f"   From: {first_input['prev_out']['addr']}")
                
                # Print first output address for reference
                if transaction.get('out') and len(transaction['out']) > 0:
                    first_output = transaction['out'][0]
                    if 'addr' in first_output:
                        print(f"   To: {first_output['addr']}")
                        print(f"   Amount: {first_output.get('value', 0) / 100000000:.8f} BTC")
                
                # Stop after collecting the required number of samples
                if tx_count >= self.sample_count:
                    print(f"\n✅ Successfully collected {self.sample_count} sample transactions!")
                    ws.close()
                    
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse JSON message: {e}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def on_error(self, ws, error):
        """Called when WebSocket encounters an error."""
        print(f"❌ WebSocket error: {error}")
        
    def on_close(self, ws, close_status_code, close_msg):
        """Called when WebSocket connection is closed."""
        if self.connection_successful:
            elapsed_time = time.time() - self.start_time if self.start_time else 0
            print(f"\n🔌 WebSocket connection closed")
            print(f"⏱️  Session duration: {elapsed_time:.2f} seconds")
            print("=" * 60)
        else:
            print(f"❌ WebSocket connection closed unexpectedly")
            if close_msg:
                print(f"   Close message: {close_msg}")
                
    def print_summary_statistics(self):
        """Print summary statistics of collected transaction data."""
        if not self.collected_transactions:
            print("❌ No transactions collected!")
            return
            
        print(f"\n📊 SUMMARY STATISTICS")
        print("=" * 60)
        print(f"Total transactions collected: {len(self.collected_transactions)}")
        
        # Calculate basic statistics
        sizes = [tx.get('size', 0) for tx in self.collected_transactions]
        input_counts = [tx.get('vin_sz', 0) for tx in self.collected_transactions]
        output_counts = [tx.get('vout_sz', 0) for tx in self.collected_transactions]
        
        if sizes:
            print(f"Average transaction size: {sum(sizes) / len(sizes):.1f} bytes")
            print(f"Size range: {min(sizes)} - {max(sizes)} bytes")
            
        if input_counts:
            print(f"Average inputs per transaction: {sum(input_counts) / len(input_counts):.1f}")
            
        if output_counts:
            print(f"Average outputs per transaction: {sum(output_counts) / len(output_counts):.1f}")
            
        # Show data structure sample
        print(f"\n🔍 SAMPLE TRANSACTION STRUCTURE:")
        print("=" * 60)
        if self.collected_transactions:
            sample_tx = self.collected_transactions[0]
            # Pretty print the first transaction (truncated for readability)
            print(json.dumps(self._truncate_transaction_for_display(sample_tx), indent=2))
    
    def _truncate_transaction_for_display(self, tx: Dict[str, Any]) -> Dict[str, Any]:
        """Truncate transaction data for display purposes."""
        truncated = tx.copy()
        
        # Limit inputs and outputs to first 2 items for display
        if 'inputs' in truncated and len(truncated['inputs']) > 2:
            truncated['inputs'] = truncated['inputs'][:2] + [{"...": f"{len(tx['inputs']) - 2} more inputs"}]
            
        if 'out' in truncated and len(truncated['out']) > 2:
            truncated['out'] = truncated['out'][:2] + [{"...": f"{len(tx['out']) - 2} more outputs"}]
            
        return truncated
    
    def test_connection(self):
        """Main method to test the WebSocket connection."""
        print("🚀 Starting Bitcoin Blockchain WebSocket Connection Test")
        print("=" * 60)
        print(f"Target URL: {self.websocket_url}")
        print(f"Sample count: {self.sample_count}")
        print(f"Connection timeout: {self.connection_timeout}s")
        print("=" * 60)
        
        try:
            # Enable WebSocket debug logging if needed (uncomment if needed for debugging)
            # websocket.enableTrace(True)
            
            # Create WebSocket connection with callbacks
            self.ws = WebSocketApp(
                self.websocket_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            # Run the WebSocket connection
            self.ws.run_forever(ping_interval=30, ping_timeout=10)
            
        except Exception as e:
            print(f"❌ Failed to establish WebSocket connection: {e}")
            return False
            
        finally:
            # Print summary regardless of success/failure
            self.print_summary_statistics()
            
        return len(self.collected_transactions) > 0


def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(consumer_group="data_producer", auto_create_topics=True)
    memory_usage_source = MemoryUsageGenerator(name="memory-usage-producer")
    output_topic = app.topic(name=os.environ["output"])

    # --- Setup Source ---
    # OPTION 1: no additional processing with a StreamingDataFrame
    # Generally the recommended approach; no additional operations needed!
    app.add_source(source=memory_usage_source, topic=output_topic)

    # OPTION 2: additional processing with a StreamingDataFrame
    # Useful for consolidating additional data cleanup into 1 Application.
    # In this case, do NOT use `app.add_source()`.
    # sdf = app.dataframe(source=source)
    # <sdf operations here>
    # sdf.to_topic(topic=output_topic) # you must do this to output your data!

    # With our pipeline defined, now run the Application
    app.run()


#  Sources require execution under a conditional main
if __name__ == "__main__":
    main()