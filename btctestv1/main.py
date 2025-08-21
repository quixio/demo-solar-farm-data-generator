"""
Bitcoin Blockchain WebSocket Connection Test

This script tests the connection to blockchain.info's WebSocket API to receive
real-time bitcoin transaction data. It reads a specified number of sample
transactions and prints them for inspection.

This is a CONNECTION TEST ONLY - no Kafka integration yet.
"""

import os
import json
import time
import threading
import websocket
from websocket import WebSocketApp
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BitcoinWebSocketTest:
    """
    A test class to connect to blockchain.info WebSocket API and collect
    sample bitcoin transaction data for inspection.
    """
    
    def __init__(self, ws_url, sample_count=10):
        self.ws_url = ws_url
        self.sample_count = sample_count
        self.collected_transactions = []
        self.ws = None
        self.connection_established = False
        self.error_occurred = None
        
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            
            # Check if this is a transaction message
            if data.get("op") == "utx" and "x" in data:
                transaction = data["x"]
                self.collected_transactions.append(transaction)
                
                print(f"\n=== Transaction {len(self.collected_transactions)}/{self.sample_count} ===")
                print(f"Hash: {transaction.get('hash', 'N/A')}")
                print(f"Time: {datetime.fromtimestamp(transaction.get('time', 0)).isoformat()}")
                print(f"Size: {transaction.get('size', 'N/A')} bytes")
                print(f"Inputs: {transaction.get('vin_sz', 'N/A')}")
                print(f"Outputs: {transaction.get('vout_sz', 'N/A')}")
                
                # Show input details
                if transaction.get('inputs'):
                    total_input_value = 0
                    print("Input addresses:")
                    for i, inp in enumerate(transaction['inputs'][:3]):  # Show first 3 inputs
                        prev_out = inp.get('prev_out', {})
                        addr = prev_out.get('addr', 'Unknown')
                        value = prev_out.get('value', 0)
                        total_input_value += value
                        print(f"  [{i+1}] {addr}: {value/100000000:.8f} BTC")
                    
                    if len(transaction['inputs']) > 3:
                        print(f"  ... and {len(transaction['inputs']) - 3} more inputs")
                
                # Show output details
                if transaction.get('out'):
                    total_output_value = 0
                    print("Output addresses:")
                    for i, out in enumerate(transaction['out'][:3]):  # Show first 3 outputs
                        addr = out.get('addr', 'Unknown')
                        value = out.get('value', 0)
                        total_output_value += value
                        print(f"  [{i+1}] {addr}: {value/100000000:.8f} BTC")
                    
                    if len(transaction['out']) > 3:
                        print(f"  ... and {len(transaction['out']) - 3} more outputs")
                
                # Check if we've collected enough samples
                if len(self.collected_transactions) >= self.sample_count:
                    print(f"\n✅ Successfully collected {self.sample_count} sample transactions!")
                    ws.close()
                    
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse message as JSON: {e}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        self.error_occurred = str(error)
        print(f"❌ WebSocket error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        if self.error_occurred:
            print(f"🔌 WebSocket connection closed due to error: {self.error_occurred}")
        else:
            print("🔌 WebSocket connection closed successfully")
    
    def on_open(self, ws):
        """Handle WebSocket open"""
        self.connection_established = True
        print("🔗 WebSocket connection established successfully!")
        print("📡 Subscribing to unconfirmed bitcoin transactions...")
        
        # Subscribe to unconfirmed transactions
        subscribe_message = {"op": "unconfirmed_sub"}
        ws.send(json.dumps(subscribe_message))
        print("✅ Subscription message sent")
    
    def test_connection(self, timeout=60):
        """
        Test the connection to the blockchain WebSocket API
        
        Args:
            timeout (int): Maximum time to wait for collecting samples in seconds
        """
        print("=== Bitcoin Blockchain WebSocket Connection Test ===")
        print(f"WebSocket URL: {self.ws_url}")
        print(f"Target samples: {self.sample_count}")
        print(f"Timeout: {timeout} seconds")
        print("\n🚀 Starting connection test...")
        
        try:
            # Enable WebSocket debug logging for troubleshooting
            # websocket.enableTrace(True)
            
            # Create WebSocket connection
            self.ws = WebSocketApp(
                self.ws_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )
            
            # Start the WebSocket in a separate thread
            ws_thread = threading.Thread(target=self.ws.run_forever)
            ws_thread.daemon = True
            ws_thread.start()
            
            # Wait for connection to establish
            connection_timeout = 10
            start_time = time.time()
            while not self.connection_established and not self.error_occurred:
                if time.time() - start_time > connection_timeout:
                    raise Exception(f"Failed to establish connection within {connection_timeout} seconds")
                time.sleep(0.1)
            
            if self.error_occurred:
                raise Exception(f"Connection failed: {self.error_occurred}")
            
            # Wait for data collection or timeout
            start_time = time.time()
            while len(self.collected_transactions) < self.sample_count:
                if time.time() - start_time > timeout:
                    print(f"⚠️  Timeout reached ({timeout}s). Collected {len(self.collected_transactions)} transactions.")
                    break
                
                if self.error_occurred:
                    raise Exception(f"Data collection failed: {self.error_occurred}")
                
                time.sleep(0.5)
            
            # Close connection
            if self.ws:
                self.ws.close()
            
            # Wait for thread to finish
            ws_thread.join(timeout=5)
            
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
        
        # Print summary
        print(f"\n=== Test Summary ===")
        print(f"✅ Connection: {'Success' if self.connection_established else 'Failed'}")
        print(f"📊 Transactions collected: {len(self.collected_transactions)}")
        print(f"⏱️  Status: {'Complete' if len(self.collected_transactions) >= self.sample_count else 'Partial'}")
        
        if self.collected_transactions:
            print(f"\n📈 Sample Data Structure (first transaction):")
            sample_tx = self.collected_transactions[0]
            print(f"Available fields: {list(sample_tx.keys())}")
            print(f"Sample transaction hash: {sample_tx.get('hash', 'N/A')}")
            print(f"Sample transaction time: {sample_tx.get('time', 'N/A')}")
            
            # Show data types for schema inference
            print(f"\n📋 Data Types for Schema Inference:")
            for key, value in sample_tx.items():
                print(f"  {key}: {type(value).__name__}")
                
        return len(self.collected_transactions) > 0


def main():
    """
    Main function to run the bitcoin WebSocket connection test
    """
    try:
        # Get configuration from environment variables
        ws_url = os.environ.get("WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
        sample_count = int(os.environ.get("SAMPLE_COUNT", "10"))
        
        print("📋 Configuration:")
        print(f"  WebSocket URL: {ws_url}")
        print(f"  Sample count: {sample_count}")
        
        # Create and run the test
        test = BitcoinWebSocketTest(ws_url, sample_count)
        success = test.test_connection(timeout=120)  # 2 minute timeout
        
        if success:
            print("\n🎉 Connection test completed successfully!")
            print("💡 Ready for Quix Streams integration in the next step.")
        else:
            print("\n❌ Connection test failed!")
            print("🔧 Check your network connection and try again.")
            
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")


if __name__ == "__main__":
    main()