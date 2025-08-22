import os
import json
import time
import threading
from datetime import datetime
from queue import Queue, Empty
from websocket import WebSocketApp

from quixstreams import Application
from quixstreams.sources import Source

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BitcoinTransactionSource(Source):
    """
    A Quix Streams Source that connects to blockchain.com WebSocket API
    to stream Bitcoin transaction data to Kafka.
    
    This source subscribes to either all unconfirmed transactions or
    transactions for a specific Bitcoin address based on configuration.
    """

    def __init__(self, websocket_url, subscription_type, bitcoin_address="", **kwargs):
        super().__init__(**kwargs)
        self.websocket_url = websocket_url
        self.subscription_type = subscription_type
        self.bitcoin_address = bitcoin_address
        self.message_queue = Queue()
        self.ws = None
        self.ws_thread = None
        self.message_count = 0
        self.max_messages = int(os.getenv('MAX_MESSAGES', 100))  # Limit for testing
        
        print(f"🔧 Bitcoin Transaction Source initialized")
        print(f"   WebSocket URL: {self.websocket_url}")
        print(f"   Subscription: {self.subscription_type}")
        if self.bitcoin_address and self.subscription_type == 'addr_sub':
            print(f"   Bitcoin Address: {self.bitcoin_address}")
        print(f"   Message Limit: {self.max_messages}")

    def _on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            raw_data = json.loads(message)
            print(f"📦 Raw WebSocket message received:")
            print(f"   Operation: {raw_data.get('op', 'Unknown')}")
            
            # Only process transaction messages
            if raw_data.get('op') == 'utx':
                # Add timestamp for when we received the message
                processed_data = self._transform_transaction_data(raw_data)
                
                if processed_data:
                    self.message_queue.put(processed_data)
                    self.message_count += 1
                    print(f"✅ Transaction queued (#{self.message_count}): {processed_data.get('transactionHash', 'N/A')}")
                    
                    # Stop after max messages for testing
                    if self.message_count >= self.max_messages:
                        print(f"🛑 Reached maximum message limit ({self.max_messages}). Closing connection.")
                        ws.close()
                
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing JSON message: {e}")
        except Exception as e:
            print(f"❌ Error processing WebSocket message: {e}")

    def _transform_transaction_data(self, raw_data):
        """Transform raw blockchain.com data to our schema format"""
        try:
            tx_data = raw_data.get('x', {})
            
            # Extract basic transaction info
            transaction = {
                'timestamp': datetime.now().isoformat(),
                'operation': raw_data.get('op', 'utx'),
                'transactionHash': tx_data.get('hash', ''),
                'transactionSize': tx_data.get('size', 0),
                'inputCount': tx_data.get('vin_sz', 0),
                'outputCount': tx_data.get('vout_sz', 0),
                'transactionTime': tx_data.get('time', 0),
                'lockTime': tx_data.get('lock_time', 0),
                'version': tx_data.get('ver', 1),
                'relayedBy': tx_data.get('relayed_by', ''),
                'txIndex': tx_data.get('tx_index', 0)
            }
            
            # Process inputs
            inputs = []
            for inp in tx_data.get('inputs', []):
                prev_out = inp.get('prev_out', {})
                input_data = {
                    'sequence': inp.get('sequence', 0),
                    'script': inp.get('script', ''),
                    'prev_out': {
                        'spent': prev_out.get('spent', False),
                        'tx_index': prev_out.get('tx_index', 0),
                        'type': prev_out.get('type', 0),
                        'addr': prev_out.get('addr', ''),
                        'value': prev_out.get('value', 0),
                        'n': prev_out.get('n', 0),
                        'script': prev_out.get('script', '')
                    }
                }
                inputs.append(input_data)
            
            transaction['inputs'] = inputs
            
            # Process outputs
            outputs = []
            for out in tx_data.get('out', []):
                output_data = {
                    'spent': out.get('spent', False),
                    'tx_index': out.get('tx_index', 0),
                    'type': out.get('type', 0),
                    'addr': out.get('addr', ''),
                    'value': out.get('value', 0),
                    'n': out.get('n', 0),
                    'script': out.get('script', '')
                }
                outputs.append(output_data)
            
            transaction['outputs'] = outputs
            
            return transaction
            
        except Exception as e:
            print(f"❌ Error transforming transaction data: {e}")
            return None

    def _on_error(self, ws, error):
        """Handle WebSocket errors"""
        print(f"❌ WebSocket Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        print(f"🔌 WebSocket connection closed")
        if close_status_code:
            print(f"   Close status: {close_status_code}")
        if close_msg:
            print(f"   Close message: {close_msg}")
        print(f"   Total transactions processed: {self.message_count}")

    def _on_open(self, ws):
        """Handle WebSocket connection open"""
        print("✅ WebSocket connection established!")
        
        try:
            # Subscribe based on configuration
            if self.subscription_type == 'addr_sub' and self.bitcoin_address:
                subscription_msg = {
                    "op": "addr_sub",
                    "addr": self.bitcoin_address
                }
                print(f"📡 Subscribing to Bitcoin address: {self.bitcoin_address}")
            else:
                subscription_msg = {
                    "op": "unconfirmed_sub"
                }
                print("📡 Subscribing to all unconfirmed transactions...")
            
            ws.send(json.dumps(subscription_msg))
            print("📡 Subscription sent successfully!")
            
        except Exception as e:
            print(f"❌ Error sending subscription: {e}")
            ws.close()

    def _start_websocket(self):
        """Start WebSocket connection in a separate thread"""
        try:
            print(f"🔗 Connecting to blockchain.com WebSocket...")
            self.ws = WebSocketApp(
                self.websocket_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            
            # Run WebSocket with keepalive
            self.ws.run_forever(ping_interval=30, ping_timeout=10)
            
        except Exception as e:
            print(f"❌ WebSocket connection failed: {e}")

    def run(self):
        """Main source run method - required by Quix Streams Source"""
        print("🚀 Starting Bitcoin Transaction Source...")
        
        # Start WebSocket connection in a separate thread
        self.ws_thread = threading.Thread(target=self._start_websocket, daemon=True)
        self.ws_thread.start()
        
        # Wait a moment for WebSocket to connect
        time.sleep(2)
        
        # Process messages from the queue and produce to Kafka
        consecutive_empty_polls = 0
        max_empty_polls = 50  # Stop after 50 empty polls (about 5 seconds)
        
        while self.running:
            try:
                # Get message from queue with timeout
                try:
                    message = self.message_queue.get(timeout=0.1)
                    consecutive_empty_polls = 0
                    
                    # Create Kafka key from transaction hash
                    key = message.get('transactionHash', 'unknown')
                    
                    # Serialize and produce the message
                    serialized = self.serialize(key=key, value=message)
                    self.produce(key=serialized.key, value=serialized.value)
                    
                    print(f"✅ Produced to Kafka: {key}")
                    
                except Empty:
                    consecutive_empty_polls += 1
                    
                    # Check if we should stop due to inactivity
                    if consecutive_empty_polls >= max_empty_polls:
                        print(f"⏱️ No messages for {max_empty_polls * 0.1} seconds")
                        
                        # If we've processed some messages and hit the limit, stop
                        if self.message_count >= self.max_messages:
                            print(f"✅ Successfully processed {self.message_count} messages. Stopping.")
                            break
                        
                        # Otherwise continue waiting
                        consecutive_empty_polls = 0
                    
                    continue
                    
            except KeyboardInterrupt:
                print("\n⚠️ Source interrupted by user")
                break
            except Exception as e:
                print(f"❌ Error in source run loop: {e}")
                time.sleep(1)  # Brief pause before retrying
        
        # Clean up WebSocket connection
        if self.ws:
            self.ws.close()
            
        print(f"🏁 Bitcoin Transaction Source stopped. Total messages: {self.message_count}")


def main():
    """Main entry point for the Bitcoin transaction source application"""
    
    print("🚀 Starting Bitcoin Blockchain Transaction Source")
    print("=" * 60)
    
    # Get configuration from environment variables
    websocket_url = os.getenv('WEBSOCKET_URL', 'wss://ws.blockchain.info/inv')
    subscription_type = os.getenv('SUBSCRIPTION_TYPE', 'unconfirmed_sub')
    bitcoin_address = os.getenv('BITCOIN_ADDRESS', '')
    output_topic_name = os.getenv('output', 'bitcoin-blockchain-transactions')
    
    print(f"Configuration:")
    print(f"  WebSocket URL: {websocket_url}")
    print(f"  Subscription Type: {subscription_type}")
    if bitcoin_address and subscription_type == 'addr_sub':
        print(f"  Bitcoin Address: {bitcoin_address}")
    print(f"  Output Topic: {output_topic_name}")
    print("-" * 60)
    
    try:
        # Create Quix Streams Application
        app = Application(
            consumer_group="bitcoin-transaction-source",
            auto_create_topics=True
        )
        
        # Create output topic
        output_topic = app.topic(
            name=output_topic_name,
            value_serializer='json'  # Use JSON serialization
        )
        
        # Create Bitcoin transaction source
        bitcoin_source = BitcoinTransactionSource(
            name="bitcoin-blockchain-source",
            websocket_url=websocket_url,
            subscription_type=subscription_type,
            bitcoin_address=bitcoin_address
        )
        
        # Set up data processing pipeline with Streaming DataFrame
        sdf = app.dataframe(source=bitcoin_source)
        
        # Add debugging output to see the messages
        sdf.print(metadata=True)
        
        # Send to output topic
        sdf.to_topic(output_topic)
        
        # Run the application
        print("🎯 Starting application...")
        app.run()
        
    except KeyboardInterrupt:
        print("\n⚠️ Application interrupted by user")
    except Exception as e:
        print(f"❌ Application failed: {e}")
        raise


if __name__ == "__main__":
    main()