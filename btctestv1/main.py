"""
Bitcoin Blockchain WebSocket Connection Test

This script connects to the Bitcoin blockchain WebSocket API to test data retrieval.
It reads 10 sample items from the real-time blockchain feed for analysis.

This is a CONNECTION TEST ONLY - no Kafka integration yet.
"""
import json
import os
import sys
import time
import websocket
from typing import Dict, Any, List

# For local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BitcoinWebSocketTester:
    """
    A test client for connecting to Bitcoin blockchain WebSocket API.
    This class handles connection, subscription, and data retrieval for testing purposes.
    """
    
    def __init__(self):
        self.websocket_url = os.environ.get('WEBSOCKET_URL', 'wss://ws.blockchain.info/inv')
        self.subscription_type = os.environ.get('SUBSCRIPTION_TYPE', 'unconfirmed_sub')
        self.connection_timeout = int(os.environ.get('CONNECTION_TIMEOUT', '30'))
        
        self.ws = None
        self.received_items = []
        self.max_items = 10
        self.connection_successful = False
        
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            self.received_items.append(data)
            
            print(f"\n--- Bitcoin Transaction #{len(self.received_items)} ---")
            
            # Format and display the transaction data
            if data.get('op') == 'utx' and 'x' in data:
                tx = data['x']
                print(f"Transaction Hash: {tx.get('hash', 'N/A')}")
                print(f"Transaction Size: {tx.get('size', 'N/A')} bytes")
                print(f"Transaction Time: {tx.get('time', 'N/A')}")
                print(f"Number of Inputs: {tx.get('vin_sz', 'N/A')}")
                print(f"Number of Outputs: {tx.get('vout_sz', 'N/A')}")
                
                # Show input details
                if 'inputs' in tx and tx['inputs']:
                    total_input_value = sum(inp.get('prev_out', {}).get('value', 0) for inp in tx['inputs'])
                    print(f"Total Input Value: {total_input_value / 100000000:.8f} BTC")
                
                # Show output details  
                if 'out' in tx and tx['out']:
                    total_output_value = sum(out.get('value', 0) for out in tx['out'])
                    print(f"Total Output Value: {total_output_value / 100000000:.8f} BTC")
                    
                    # Show first few output addresses
                    print("Output Addresses:")
                    for i, out in enumerate(tx['out'][:3]):  # Show max 3 addresses
                        addr = out.get('addr', 'N/A')
                        value_btc = out.get('value', 0) / 100000000
                        print(f"  {i+1}. {addr}: {value_btc:.8f} BTC")
                        
            elif data.get('op') == 'block' and 'x' in data:
                block = data['x']
                print(f"Block Hash: {block.get('hash', 'N/A')}")
                print(f"Block Height: {block.get('height', 'N/A')}")
                print(f"Block Time: {block.get('time', 'N/A')}")
                print(f"Number of Transactions: {block.get('nTx', 'N/A')}")
                print(f"Block Size: {block.get('size', 'N/A')} bytes")
                
            else:
                print(f"Raw message: {json.dumps(data, indent=2)}")
                
            print("-" * 60)
            
            # Stop after receiving the desired number of items
            if len(self.received_items) >= self.max_items:
                print(f"\n✅ Successfully received {self.max_items} items from Bitcoin blockchain!")
                self.print_summary()
                ws.close()
                
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing JSON message: {e}")
            print(f"Raw message: {message}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
            
    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        print(f"❌ WebSocket error: {error}")
        
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        print(f"\n🔌 WebSocket connection closed")
        if close_status_code:
            print(f"Close status code: {close_status_code}")
        if close_msg:
            print(f"Close message: {close_msg}")
            
    def on_open(self, ws):
        """Handle WebSocket connection open"""
        print(f"✅ Connected to Bitcoin blockchain WebSocket: {self.websocket_url}")
        self.connection_successful = True
        
        # Subscribe to the specified data type
        subscription_message = {"op": self.subscription_type}
        
        print(f"📡 Subscribing to: {self.subscription_type}")
        ws.send(json.dumps(subscription_message))
        
        if self.subscription_type == 'unconfirmed_sub':
            print("🔄 Waiting for unconfirmed Bitcoin transactions...")
        elif self.subscription_type == 'blocks_sub':
            print("🔄 Waiting for new Bitcoin blocks...")
        else:
            print(f"🔄 Waiting for data from subscription: {self.subscription_type}")
            
    def print_summary(self):
        """Print a summary of the connection test"""
        print(f"\n{'='*60}")
        print("CONNECTION TEST SUMMARY")
        print(f"{'='*60}")
        print(f"WebSocket URL: {self.websocket_url}")
        print(f"Subscription Type: {self.subscription_type}")
        print(f"Connection Successful: {'✅ Yes' if self.connection_successful else '❌ No'}")
        print(f"Items Received: {len(self.received_items)}")
        print(f"Test Status: {'✅ PASSED' if len(self.received_items) >= self.max_items else '⚠️ INCOMPLETE'}")
        
        if self.received_items:
            print(f"\nData Structure Analysis:")
            first_item = self.received_items[0]
            print(f"- Message Type: {first_item.get('op', 'Unknown')}")
            
            if 'x' in first_item:
                data_fields = list(first_item['x'].keys())
                print(f"- Available Fields: {', '.join(data_fields[:10])}{'...' if len(data_fields) > 10 else ''}")
                
        print(f"{'='*60}")
        
    def test_connection(self):
        """Main method to test the WebSocket connection"""
        print(f"🚀 Starting Bitcoin Blockchain WebSocket Connection Test")
        print(f"📍 Target: {self.websocket_url}")
        print(f"🎯 Subscription: {self.subscription_type}")
        print(f"📊 Target samples: {self.max_items}")
        print("-" * 60)
        
        try:
            # Enable WebSocket tracing for debugging (comment out for production)
            # websocket.enableTrace(True)
            
            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                self.websocket_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )
            
            # Run the WebSocket connection with timeout
            print(f"⏳ Attempting connection (timeout: {self.connection_timeout}s)...")
            self.ws.run_forever(ping_timeout=self.connection_timeout)
            
        except KeyboardInterrupt:
            print("\n⏹️ Connection test interrupted by user")
            self.print_summary()
        except Exception as e:
            print(f"❌ Connection error: {e}")
            print(f"❌ Failed to connect to Bitcoin blockchain WebSocket")
            return False
            
        return len(self.received_items) >= self.max_items


def main():
    """Main function to run the Bitcoin blockchain connection test"""
    print("Bitcoin Blockchain WebSocket Connection Tester")
    print("=" * 50)
    
    # Validate required environment variables
    required_env_vars = ['WEBSOCKET_URL', 'SUBSCRIPTION_TYPE']
    missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set the required environment variables and try again.")
        return False
    
    # Create and run the tester
    tester = BitcoinWebSocketTester()
    success = tester.test_connection()
    
    if success:
        print("\n🎉 Connection test completed successfully!")
        return True
    else:
        print("\n❌ Connection test failed or incomplete.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)