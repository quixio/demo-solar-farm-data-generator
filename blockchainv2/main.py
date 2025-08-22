"""
Bitcoin Blockchain.com WebSocket Connection Test

This script tests the connection to blockchain.com's WebSocket API to read Bitcoin transaction data.
This is a CONNECTION TEST ONLY - no Kafka integration yet.
"""

import os
import json
import time
import threading
from datetime import datetime
import websocket

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BlockchainWebSocketTester:
    """
    Test connection to blockchain.com WebSocket API.
    
    This class connects to the blockchain.com WebSocket and subscribes to Bitcoin transaction data.
    It collects exactly 10 sample transactions for inspection and then disconnects.
    """

    def __init__(self):
        self.ws = None
        self.connected = False
        self.sample_count = 0
        self.target_samples = 10
        self.samples = []
        self.connection_timeout = int(os.environ.get("CONNECTION_TIMEOUT", "30"))
        
        # Configuration from environment variables
        self.websocket_url = os.environ.get("WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
        self.subscription_type = os.environ.get("SUBSCRIPTION_TYPE", "unconfirmed_sub")
        self.bitcoin_address = os.environ.get("BITCOIN_ADDRESS", "")
        
        print(f"=== Blockchain.com WebSocket Connection Test ===")
        print(f"WebSocket URL: {self.websocket_url}")
        print(f"Subscription Type: {self.subscription_type}")
        if self.bitcoin_address and self.subscription_type == "addr_sub":
            print(f"Monitoring Address: {self.bitcoin_address}")
        print(f"Target Samples: {self.target_samples}")
        print(f"Connection Timeout: {self.connection_timeout} seconds")
        print("-" * 50)

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            data = json.loads(message)
            
            # Only process transaction messages (op: utx) or block messages (op: block)
            if data.get("op") == "utx" and self.sample_count < self.target_samples:
                self.sample_count += 1
                transaction = data.get("x", {})
                
                # Extract key transaction information for display
                sample_data = {
                    "sample_number": self.sample_count,
                    "timestamp": datetime.now().isoformat(),
                    "transaction_hash": transaction.get("hash", "N/A"),
                    "transaction_index": transaction.get("tx_index", "N/A"),
                    "size": transaction.get("size", 0),
                    "input_count": transaction.get("vin_sz", 0),
                    "output_count": transaction.get("vout_sz", 0),
                    "time": transaction.get("time", 0),
                    "relayed_by": transaction.get("relayed_by", "N/A"),
                    "lock_time": transaction.get("lock_time", 0),
                    "version": transaction.get("ver", 0)
                }
                
                # Calculate total input and output values
                total_input_value = 0
                total_output_value = 0
                
                for inp in transaction.get("inputs", []):
                    if inp.get("prev_out") and inp["prev_out"].get("value"):
                        total_input_value += inp["prev_out"]["value"]
                
                for out in transaction.get("out", []):
                    if out.get("value"):
                        total_output_value += out["value"]
                
                sample_data["total_input_value_satoshi"] = total_input_value
                sample_data["total_output_value_satoshi"] = total_output_value
                sample_data["total_input_value_btc"] = total_input_value / 100000000  # Convert satoshi to BTC
                sample_data["total_output_value_btc"] = total_output_value / 100000000
                
                # Store the sample
                self.samples.append(sample_data)
                
                # Display the sample
                print(f"\n📦 SAMPLE {self.sample_count}/{self.target_samples} - Bitcoin Transaction")
                print(f"   Hash: {sample_data['transaction_hash']}")
                print(f"   Index: {sample_data['transaction_index']}")
                print(f"   Time: {datetime.fromtimestamp(sample_data['time']) if sample_data['time'] > 0 else 'N/A'}")
                print(f"   Size: {sample_data['size']} bytes")
                print(f"   Inputs: {sample_data['input_count']}, Outputs: {sample_data['output_count']}")
                print(f"   Total Value: {sample_data['total_output_value_btc']:.8f} BTC")
                print(f"   Relayed by: {sample_data['relayed_by']}")
                
                if self.sample_count >= self.target_samples:
                    print(f"\n✅ Successfully collected {self.target_samples} samples!")
                    self.disconnect()
                    
            elif data.get("op") == "block" and self.subscription_type == "blocks_sub":
                if self.sample_count < self.target_samples:
                    self.sample_count += 1
                    block = data.get("x", {})
                    
                    sample_data = {
                        "sample_number": self.sample_count,
                        "timestamp": datetime.now().isoformat(),
                        "block_hash": block.get("hash", "N/A"),
                        "block_index": block.get("blockIndex", "N/A"),
                        "height": block.get("height", 0),
                        "num_transactions": block.get("nTx", 0),
                        "size": block.get("size", 0),
                        "time": block.get("time", 0),
                        "merkle_root": block.get("mrklRoot", "N/A"),
                        "version": block.get("version", 0),
                        "bits": block.get("bits", 0),
                        "nonce": block.get("nonce", 0)
                    }
                    
                    self.samples.append(sample_data)
                    
                    print(f"\n🧱 SAMPLE {self.sample_count}/{self.target_samples} - Bitcoin Block")
                    print(f"   Hash: {sample_data['block_hash']}")
                    print(f"   Height: {sample_data['height']}")
                    print(f"   Time: {datetime.fromtimestamp(sample_data['time']) if sample_data['time'] > 0 else 'N/A'}")
                    print(f"   Transactions: {sample_data['num_transactions']}")
                    print(f"   Size: {sample_data['size']} bytes")
                    
                    if self.sample_count >= self.target_samples:
                        print(f"\n✅ Successfully collected {self.target_samples} samples!")
                        self.disconnect()
            
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing message: {e}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")

    def on_error(self, ws, error):
        """Handle WebSocket errors."""
        print(f"❌ WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close."""
        self.connected = False
        print(f"\n🔌 WebSocket connection closed")
        if close_status_code:
            print(f"   Status code: {close_status_code}")
        if close_msg:
            print(f"   Message: {close_msg}")

    def on_open(self, ws):
        """Handle WebSocket connection open."""
        self.connected = True
        print("✅ WebSocket connection established!")
        
        # Subscribe based on subscription type
        try:
            if self.subscription_type == "unconfirmed_sub":
                # Subscribe to all unconfirmed transactions
                subscription_msg = {"op": "unconfirmed_sub"}
                print("📡 Subscribing to unconfirmed transactions...")
                
            elif self.subscription_type == "addr_sub":
                # Subscribe to transactions for a specific address
                if not self.bitcoin_address:
                    print("❌ Bitcoin address is required for addr_sub subscription type")
                    self.disconnect()
                    return
                subscription_msg = {"op": "addr_sub", "addr": self.bitcoin_address}
                print(f"📡 Subscribing to transactions for address: {self.bitcoin_address}")
                
            elif self.subscription_type == "blocks_sub":
                # Subscribe to new blocks
                subscription_msg = {"op": "blocks_sub"}
                print("📡 Subscribing to new blocks...")
                
            else:
                print(f"❌ Unknown subscription type: {self.subscription_type}")
                self.disconnect()
                return
                
            # Send subscription message
            ws.send(json.dumps(subscription_msg))
            print(f"✅ Subscription message sent: {subscription_msg}")
            print("⏳ Waiting for data...")
            
        except Exception as e:
            print(f"❌ Error sending subscription: {e}")
            self.disconnect()

    def connect(self):
        """Establish WebSocket connection."""
        try:
            print("🔄 Connecting to blockchain.com WebSocket...")
            
            # Enable WebSocket debugging (optional)
            # websocket.enableTrace(True)
            
            self.ws = websocket.WebSocketApp(
                self.websocket_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )
            
            # Set up timeout
            def timeout_handler():
                time.sleep(self.connection_timeout)
                if self.sample_count < self.target_samples:
                    print(f"\n⏰ Connection timeout ({self.connection_timeout}s) reached")
                    print(f"   Collected {self.sample_count}/{self.target_samples} samples")
                    self.disconnect()
            
            timeout_thread = threading.Thread(target=timeout_handler)
            timeout_thread.daemon = True
            timeout_thread.start()
            
            # Run the WebSocket connection
            self.ws.run_forever()
            
        except Exception as e:
            print(f"❌ Connection error: {e}")

    def disconnect(self):
        """Disconnect from WebSocket."""
        if self.ws and self.connected:
            try:
                # Unsubscribe before closing
                if self.subscription_type == "unconfirmed_sub":
                    self.ws.send(json.dumps({"op": "unconfirmed_unsub"}))
                elif self.subscription_type == "addr_sub" and self.bitcoin_address:
                    self.ws.send(json.dumps({"op": "addr_unsub", "addr": self.bitcoin_address}))
                elif self.subscription_type == "blocks_sub":
                    self.ws.send(json.dumps({"op": "blocks_unsub"}))
                    
                time.sleep(0.5)  # Give time for unsubscribe message
                self.ws.close()
            except Exception as e:
                print(f"⚠️  Error during disconnect: {e}")

    def print_summary(self):
        """Print test summary and collected samples."""
        print(f"\n{'='*60}")
        print(f"CONNECTION TEST SUMMARY")
        print(f"{'='*60}")
        print(f"Websocket URL: {self.websocket_url}")
        print(f"Subscription Type: {self.subscription_type}")
        print(f"Samples Collected: {len(self.samples)}/{self.target_samples}")
        print(f"Test Status: {'✅ SUCCESS' if len(self.samples) > 0 else '❌ FAILED'}")
        
        if self.samples:
            print(f"\n📊 SAMPLE DATA STRUCTURE ANALYSIS:")
            print(f"   - Each sample contains: {len(self.samples[0])} fields")
            print(f"   - Sample fields: {', '.join(self.samples[0].keys())}")
            
            if self.subscription_type in ["unconfirmed_sub", "addr_sub"]:
                total_btc = sum(sample.get("total_output_value_btc", 0) for sample in self.samples)
                avg_btc = total_btc / len(self.samples) if self.samples else 0
                print(f"   - Average transaction value: {avg_btc:.8f} BTC")
                print(f"   - Total value observed: {total_btc:.8f} BTC")
            
        print(f"\n🔍 SAMPLE DETAILS:")
        for i, sample in enumerate(self.samples[:5], 1):  # Show first 5 samples
            print(f"\nSample {i}:")
            for key, value in sample.items():
                if key not in ['sample_number', 'timestamp']:
                    print(f"   {key}: {value}")
                    
        if len(self.samples) > 5:
            print(f"\n... ({len(self.samples) - 5} more samples collected)")


def main():
    """Main function to run the blockchain.com WebSocket connection test."""
    print("🚀 Starting Blockchain.com WebSocket Connection Test")
    
    # Validate environment variables
    required_vars = ["WEBSOCKET_URL", "SUBSCRIPTION_TYPE"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        return
    
    # Initialize and run the test
    tester = BlockchainWebSocketTester()
    
    try:
        tester.connect()
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
    finally:
        tester.print_summary()
        print("\n🏁 Test completed")


if __name__ == "__main__":
    main()