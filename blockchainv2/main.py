import os
import json
import time
import threading
from datetime import datetime
import websocket
from websocket import enableTrace

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BlockchainWebSocketTester:
    """
    Connection tester for Blockchain.com WebSocket API.
    This class will connect to the WebSocket, subscribe to Bitcoin transaction data,
    and collect exactly 10 sample transactions for inspection.
    """
    
    def __init__(self):
        self.websocket_url = os.environ.get("BLOCKCHAIN_WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
        self.subscription_type = os.environ.get("SUBSCRIPTION_TYPE", "unconfirmed_sub")
        self.bitcoin_address = os.environ.get("BITCOIN_ADDRESS", "")
        
        self.ws = None
        self.received_messages = []
        self.max_messages = 10
        self.connection_established = False
        self.error_occurred = False
        self.error_message = ""
        
    def on_open(self, ws):
        """Called when WebSocket connection is opened"""
        print(f"✅ Successfully connected to {self.websocket_url}")
        print(f"📡 Connection established at {datetime.now().isoformat()}")
        self.connection_established = True
        
        # Subscribe based on configuration
        if self.subscription_type == "addr_sub" and self.bitcoin_address:
            subscription_msg = {
                "op": "addr_sub",
                "addr": self.bitcoin_address
            }
            print(f"🔔 Subscribing to transactions for Bitcoin address: {self.bitcoin_address}")
        else:
            subscription_msg = {
                "op": "unconfirmed_sub"
            }
            print("🔔 Subscribing to all unconfirmed Bitcoin transactions")
            
        try:
            ws.send(json.dumps(subscription_msg))
            print("📤 Subscription message sent successfully")
        except Exception as e:
            self.error_occurred = True
            self.error_message = f"Failed to send subscription message: {str(e)}"
            print(f"❌ {self.error_message}")
    
    def on_message(self, ws, message):
        """Called when a message is received from WebSocket"""
        try:
            data = json.loads(message)
            
            # Only collect transaction messages (op: utx)
            if data.get("op") == "utx" and len(self.received_messages) < self.max_messages:
                self.received_messages.append(data)
                
                # Extract key transaction info for display
                tx_data = data.get("x", {})
                tx_hash = tx_data.get("hash", "N/A")
                tx_size = tx_data.get("size", 0)
                tx_time = tx_data.get("time", 0)
                input_count = tx_data.get("vin_sz", 0)
                output_count = tx_data.get("vout_sz", 0)
                
                # Calculate total value
                total_value = 0
                if "out" in tx_data:
                    for output in tx_data["out"]:
                        total_value += output.get("value", 0)
                
                print(f"\n📦 Transaction {len(self.received_messages)}/10:")
                print(f"   Hash: {tx_hash}")
                print(f"   Size: {tx_size} bytes")
                print(f"   Time: {datetime.fromtimestamp(tx_time).isoformat() if tx_time else 'N/A'}")
                print(f"   Inputs: {input_count}, Outputs: {output_count}")
                print(f"   Total Value: {total_value / 100000000:.8f} BTC")
                
                # Close connection after collecting enough samples
                if len(self.received_messages) >= self.max_messages:
                    print(f"\n✅ Successfully collected {self.max_messages} sample transactions!")
                    ws.close()
                    
        except json.JSONDecodeError as e:
            print(f"⚠️ Failed to parse message as JSON: {e}")
        except Exception as e:
            self.error_occurred = True
            self.error_message = f"Error processing message: {str(e)}"
            print(f"❌ {self.error_message}")
    
    def on_error(self, ws, error):
        """Called when WebSocket encounters an error"""
        self.error_occurred = True
        self.error_message = f"WebSocket error: {str(error)}"
        print(f"❌ {self.error_message}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Called when WebSocket connection is closed"""
        if close_status_code:
            print(f"🔌 WebSocket connection closed with code: {close_status_code}")
            if close_msg:
                print(f"   Close message: {close_msg}")
        else:
            print("🔌 WebSocket connection closed normally")
    
    def test_connection(self, timeout=30):
        """
        Test the connection to Blockchain.com WebSocket API
        
        Args:
            timeout: Maximum time to wait for collecting samples (seconds)
        
        Returns:
            bool: True if test was successful, False otherwise
        """
        print("🚀 Starting Blockchain.com WebSocket API Connection Test")
        print("=" * 60)
        print(f"WebSocket URL: {self.websocket_url}")
        print(f"Subscription Type: {self.subscription_type}")
        if self.bitcoin_address:
            print(f"Bitcoin Address: {self.bitcoin_address}")
        print(f"Target Sample Count: {self.max_messages}")
        print("=" * 60)
        
        try:
            # Enable WebSocket debugging for more verbose output
            enableTrace(False)
            
            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                self.websocket_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            # Start the WebSocket in a separate thread
            wst = threading.Thread(target=self.ws.run_forever)
            wst.daemon = True
            wst.start()
            
            # Wait for connection or timeout
            start_time = time.time()
            while not self.connection_established and not self.error_occurred:
                if time.time() - start_time > 10:  # 10 seconds to establish connection
                    print("❌ Connection timeout - could not establish WebSocket connection")
                    return False
                time.sleep(0.1)
            
            if self.error_occurred:
                print(f"❌ Connection failed: {self.error_message}")
                return False
            
            # Wait for messages or timeout
            while len(self.received_messages) < self.max_messages and not self.error_occurred:
                if time.time() - start_time > timeout:
                    print(f"⏰ Timeout reached ({timeout}s) - collected {len(self.received_messages)} samples")
                    break
                time.sleep(0.1)
            
            return len(self.received_messages) > 0 and not self.error_occurred
            
        except Exception as e:
            print(f"❌ Unexpected error during connection test: {str(e)}")
            return False
        finally:
            if self.ws:
                self.ws.close()
    
    def print_sample_data(self):
        """Print detailed information about collected sample data"""
        if not self.received_messages:
            print("❌ No sample data collected")
            return
            
        print(f"\n📊 SAMPLE DATA ANALYSIS ({len(self.received_messages)} transactions)")
        print("=" * 80)
        
        for i, msg in enumerate(self.received_messages, 1):
            tx_data = msg.get("x", {})
            print(f"\n📦 TRANSACTION {i}:")
            print("-" * 40)
            print(f"Hash: {tx_data.get('hash', 'N/A')}")
            print(f"Version: {tx_data.get('ver', 'N/A')}")
            print(f"Size: {tx_data.get('size', 'N/A')} bytes")
            print(f"Lock Time: {tx_data.get('lock_time', 'N/A')}")
            print(f"Time: {datetime.fromtimestamp(tx_data.get('time', 0)).isoformat() if tx_data.get('time') else 'N/A'}")
            print(f"Transaction Index: {tx_data.get('tx_index', 'N/A')}")
            print(f"Relayed By: {tx_data.get('relayed_by', 'N/A')}")
            
            # Input details
            inputs = tx_data.get('inputs', [])
            print(f"Inputs ({len(inputs)}):")
            for j, inp in enumerate(inputs):
                prev_out = inp.get('prev_out', {})
                print(f"  Input {j+1}: {prev_out.get('addr', 'N/A')} -> {prev_out.get('value', 0) / 100000000:.8f} BTC")
            
            # Output details
            outputs = tx_data.get('out', [])
            print(f"Outputs ({len(outputs)}):")
            total_out = 0
            for j, out in enumerate(outputs):
                value = out.get('value', 0)
                total_out += value
                print(f"  Output {j+1}: {out.get('addr', 'N/A')} -> {value / 100000000:.8f} BTC")
            
            print(f"Total Output Value: {total_out / 100000000:.8f} BTC")
        
        print(f"\n✅ DATA STRUCTURE SUMMARY:")
        print("-" * 30)
        print("Each transaction contains:")
        print("• Transaction metadata (hash, size, time, version)")
        print("• Input transactions with previous outputs")
        print("• Output transactions with addresses and values")
        print("• Network information (relayed_by)")
        print("• Indexing information (tx_index)")
        
        if self.received_messages:
            sample_tx = self.received_messages[0].get('x', {})
            print(f"\n🔍 SAMPLE TRANSACTION FIELDS:")
            print("-" * 35)
            for key in sorted(sample_tx.keys()):
                value = sample_tx[key]
                if isinstance(value, list) and value:
                    print(f"• {key}: array with {len(value)} items")
                elif isinstance(value, dict):
                    print(f"• {key}: object with {len(value)} fields")
                else:
                    print(f"• {key}: {type(value).__name__}")


def main():
    """Main function to test the Blockchain.com WebSocket connection"""
    print("🎯 BLOCKCHAIN.COM WEBSOCKET API CONNECTION TEST")
    print("=" * 60)
    
    # Initialize the tester
    tester = BlockchainWebSocketTester()
    
    # Test the connection
    success = tester.test_connection(timeout=60)  # 60 seconds timeout
    
    if success:
        print("\n🎉 CONNECTION TEST SUCCESSFUL!")
        print(f"📈 Collected {len(tester.received_messages)} sample transactions")
        
        # Print detailed sample data
        tester.print_sample_data()
        
        print(f"\n✅ NEXT STEPS:")
        print("• The connection to Blockchain.com WebSocket API is working")
        print("• Sample Bitcoin transaction data has been collected")
        print("• Data structure analysis shows transaction details")
        print("• Ready for integration with Quix Streams and Kafka")
        
    else:
        print("\n❌ CONNECTION TEST FAILED!")
        if tester.error_message:
            print(f"Error: {tester.error_message}")
        
        print("\n🔧 TROUBLESHOOTING TIPS:")
        print("• Check internet connection")
        print("• Verify WebSocket URL is accessible")
        print("• Ensure firewall allows WebSocket connections")
        print("• Try again later (service might be temporarily unavailable)")


if __name__ == "__main__":
    main()