import os
import json
import time
import websocket
import threading
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BlockchainWebSocketTester:
    """
    A connection tester for blockchain.com WebSocket API.
    This class connects to the blockchain.com WebSocket API and retrieves sample bitcoin transaction data.
    
    This is a connection test only - no Kafka integration is performed.
    """
    
    def __init__(self):
        # Load environment variables
        self.websocket_url = os.environ.get("WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
        self.subscription_type = os.environ.get("SUBSCRIPTION_TYPE", "unconfirmed_sub")
        self.bitcoin_address = os.environ.get("BITCOIN_ADDRESS", "")
        self.max_messages = int(os.environ.get("MAX_MESSAGES", "10"))
        
        # Initialize counters and storage
        self.message_count = 0
        self.collected_messages = []
        self.ws = None
        self.is_connected = False
        
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            self.message_count += 1
            data = json.loads(message)
            
            # Store the message for analysis
            self.collected_messages.append({
                "timestamp": datetime.now().isoformat(),
                "message_number": self.message_count,
                "data": data
            })
            
            # Print formatted message
            print(f"\n{'='*60}")
            print(f"MESSAGE #{self.message_count}")
            print(f"TIMESTAMP: {datetime.now().isoformat()}")
            print(f"{'='*60}")
            
            if data.get("op") == "utx":
                # Unconfirmed transaction
                tx = data.get("x", {})
                print(f"TRANSACTION TYPE: Unconfirmed Transaction")
                print(f"TRANSACTION HASH: {tx.get('hash', 'N/A')}")
                print(f"TRANSACTION SIZE: {tx.get('size', 'N/A')} bytes")
                print(f"INPUTS COUNT: {tx.get('vin_sz', 'N/A')}")
                print(f"OUTPUTS COUNT: {tx.get('vout_sz', 'N/A')}")
                print(f"TIME: {tx.get('time', 'N/A')}")
                
                # Show input/output summary
                if tx.get('inputs'):
                    total_input_value = sum(inp.get('prev_out', {}).get('value', 0) for inp in tx.get('inputs', []))
                    print(f"TOTAL INPUT VALUE: {total_input_value / 100000000:.8f} BTC")
                
                if tx.get('out'):
                    total_output_value = sum(out.get('value', 0) for out in tx.get('out', []))
                    print(f"TOTAL OUTPUT VALUE: {total_output_value / 100000000:.8f} BTC")
                
            elif data.get("op") == "block":
                # New block
                block = data.get("x", {})
                print(f"MESSAGE TYPE: New Block")
                print(f"BLOCK HASH: {block.get('hash', 'N/A')}")
                print(f"BLOCK HEIGHT: {block.get('height', 'N/A')}")
                print(f"BLOCK SIZE: {block.get('size', 'N/A')} bytes")
                print(f"TRANSACTION COUNT: {block.get('nTx', 'N/A')}")
                print(f"TIMESTAMP: {block.get('time', 'N/A')}")
                print(f"MERKLE ROOT: {block.get('mrklRoot', 'N/A')}")
                
            else:
                print(f"MESSAGE TYPE: {data.get('op', 'Unknown')}")
                print(f"RAW DATA: {json.dumps(data, indent=2)}")
            
            print(f"{'='*60}\n")
            
            # Stop after collecting enough messages
            if self.message_count >= self.max_messages:
                print(f"✅ Successfully collected {self.max_messages} messages. Closing connection...")
                ws.close()
                
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse JSON message: {e}")
            print(f"Raw message: {message}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
            
    def on_error(self, ws, error):
        """Handle WebSocket errors."""
        print(f"❌ WebSocket error: {error}")
        
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close."""
        print(f"\n🔌 WebSocket connection closed")
        if close_status_code:
            print(f"Close code: {close_status_code}")
        if close_msg:
            print(f"Close message: {close_msg}")
        self.is_connected = False
        
    def on_open(self, ws):
        """Handle WebSocket open."""
        print(f"✅ Connected to blockchain.com WebSocket: {self.websocket_url}")
        self.is_connected = True
        
        try:
            # Subscribe to the specified channel
            if self.subscription_type == "addr_sub":
                if not self.bitcoin_address:
                    print("❌ Bitcoin address is required for addr_sub subscription type")
                    ws.close()
                    return
                    
                subscription_msg = {
                    "op": self.subscription_type,
                    "addr": self.bitcoin_address
                }
                print(f"📡 Subscribing to address: {self.bitcoin_address}")
            else:
                subscription_msg = {
                    "op": self.subscription_type
                }
                print(f"📡 Subscribing to: {self.subscription_type}")
            
            ws.send(json.dumps(subscription_msg))
            print(f"🎯 Subscription message sent successfully")
            print(f"⏳ Waiting for {self.max_messages} messages...")
            
        except Exception as e:
            print(f"❌ Failed to send subscription message: {e}")
            ws.close()
    
    def test_connection(self):
        """Test the WebSocket connection and collect sample data."""
        print(f"🚀 Starting Blockchain.com WebSocket Connection Test")
        print(f"{'='*80}")
        print(f"WebSocket URL: {self.websocket_url}")
        print(f"Subscription Type: {self.subscription_type}")
        print(f"Max Messages to Collect: {self.max_messages}")
        if self.bitcoin_address:
            print(f"Bitcoin Address: {self.bitcoin_address}")
        print(f"{'='*80}\n")
        
        try:
            # Create WebSocket connection
            websocket.enableTrace(False)  # Set to True for debugging
            self.ws = websocket.WebSocketApp(
                self.websocket_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            # Run the WebSocket in a separate thread with timeout
            timeout_seconds = 60  # Maximum time to wait
            
            def run_with_timeout():
                self.ws.run_forever()
            
            ws_thread = threading.Thread(target=run_with_timeout)
            ws_thread.daemon = True
            ws_thread.start()
            
            # Wait for connection or timeout
            start_time = time.time()
            while ws_thread.is_alive() and (time.time() - start_time) < timeout_seconds:
                time.sleep(0.1)
            
            if ws_thread.is_alive():
                print(f"⏰ Connection timeout after {timeout_seconds} seconds")
                if self.ws:
                    self.ws.close()
                    
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
            
        finally:
            # Print summary
            self.print_summary()
            
    def print_summary(self):
        """Print a summary of the connection test."""
        print(f"\n{'='*80}")
        print(f"📊 CONNECTION TEST SUMMARY")
        print(f"{'='*80}")
        print(f"Total Messages Collected: {self.message_count}")
        print(f"Connection Successful: {'✅ Yes' if self.is_connected else '❌ No'}")
        print(f"Test Completed: {'✅ Yes' if self.message_count >= self.max_messages else '⚠️ Incomplete'}")
        
        if self.collected_messages:
            print(f"\n📈 DATA STRUCTURE ANALYSIS:")
            print(f"{'='*40}")
            
            # Analyze message types
            message_types = {}
            for msg in self.collected_messages:
                op_type = msg['data'].get('op', 'unknown')
                message_types[op_type] = message_types.get(op_type, 0) + 1
            
            print(f"Message Types Found:")
            for msg_type, count in message_types.items():
                print(f"  - {msg_type}: {count} messages")
            
            # Show sample data structure
            if self.collected_messages:
                print(f"\n🔍 SAMPLE DATA STRUCTURE (First Message):")
                print(f"{'='*50}")
                sample_data = self.collected_messages[0]['data']
                print(json.dumps(sample_data, indent=2)[:1000] + "...")
        
        print(f"{'='*80}")
        
        if self.message_count > 0:
            print(f"✅ Connection test PASSED - Successfully received {self.message_count} messages")
        else:
            print(f"❌ Connection test FAILED - No messages received")


def main():
    """Main function to run the blockchain WebSocket connection test."""
    try:
        tester = BlockchainWebSocketTester()
        tester.test_connection()
    except KeyboardInterrupt:
        print(f"\n⚠️ Test interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()