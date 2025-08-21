# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sources, see https://quix.io/docs/quix-streams/connectors/sources/index.html
from quixstreams import Application
from quixstreams.sources import Source

import os
import json
import time
import websocket
from typing import Dict, Any, Optional


class MemoryUsageGenerator(Source):
    """
    A connection tester for blockchain.com WebSocket API.
    Tests the ability to connect and read Bitcoin transaction data.
    
    This is for connection testing only - no Kafka integration yet.
    """
    
    def __init__(self, ws_url: str, subscription_type: str, bitcoin_address: Optional[str] = None):
        self.ws_url = ws_url
        self.subscription_type = subscription_type
        self.bitcoin_address = bitcoin_address
        self.connection_established = False
        self.sample_count = 0
        self.max_samples = 10
        self.samples_collected = []
        
    def on_open(self, ws):
        """Called when WebSocket connection is opened."""
        print("✅ WebSocket connection established successfully!")
        print(f"Connected to: {self.ws_url}")
        self.connection_established = True
        
        # Subscribe based on subscription type
        if self.subscription_type == "unconfirmed_sub":
            subscription_message = {"op": "unconfirmed_sub"}
            print("📡 Subscribing to all unconfirmed Bitcoin transactions...")
        elif self.subscription_type == "addr_sub" and self.bitcoin_address:
            subscription_message = {"op": "addr_sub", "addr": self.bitcoin_address}
            print(f"📡 Subscribing to transactions for address: {self.bitcoin_address}")
        else:
            print("❌ Invalid subscription type or missing Bitcoin address")
            ws.close()
            return
            
        ws.send(json.dumps(subscription_message))
        print(f"Subscription message sent: {subscription_message}")
        
    def on_message(self, ws, message):
        """Called when a message is received from WebSocket."""
        try:
            data = json.loads(message)
            self.sample_count += 1
            
            print(f"\n📦 Sample {self.sample_count}/10 - New Transaction Data:")
            print("=" * 60)
            
            if data.get("op") == "utx" and "x" in data:
                transaction = data["x"]
                self.samples_collected.append(transaction)
                
                # Format and display key transaction information
                print(f"Transaction Hash: {transaction.get('hash', 'N/A')}")
                print(f"Transaction Index: {transaction.get('tx_index', 'N/A')}")
                print(f"Size: {transaction.get('size', 'N/A')} bytes")
                print(f"Version: {transaction.get('ver', 'N/A')}")
                print(f"Lock Time: {transaction.get('lock_time', 'N/A')}")
                print(f"Timestamp: {transaction.get('time', 'N/A')}")
                print(f"Relayed By: {transaction.get('relayed_by', 'N/A')}")
                
                # Input information
                inputs = transaction.get('inputs', [])
                print(f"Inputs ({len(inputs)}):")
                for i, inp in enumerate(inputs[:2]):  # Show first 2 inputs
                    prev_out = inp.get('prev_out', {})
                    print(f"  Input {i+1}: {prev_out.get('addr', 'N/A')} -> {prev_out.get('value', 0)/100000000:.8f} BTC")
                
                # Output information
                outputs = transaction.get('out', [])
                print(f"Outputs ({len(outputs)}):")
                for i, out in enumerate(outputs[:2]):  # Show first 2 outputs
                    print(f"  Output {i+1}: {out.get('addr', 'N/A')} -> {out.get('value', 0)/100000000:.8f} BTC")
                
                total_value = sum(out.get('value', 0) for out in outputs)
                print(f"Total Transaction Value: {total_value/100000000:.8f} BTC")
                
            else:
                # Handle other message types (blocks, etc.)
                print(f"Message Type: {data.get('op', 'unknown')}")
                print(f"Raw Data: {json.dumps(data, indent=2)[:500]}...")
                self.samples_collected.append(data)
                
            print("=" * 60)
            
            # Stop after collecting the desired number of samples
            if self.sample_count >= self.max_samples:
                print(f"\n🎉 Successfully collected {self.max_samples} samples!")
                print("\n📊 Connection Test Summary:")
                print(f"   • WebSocket URL: {self.ws_url}")
                print(f"   • Subscription Type: {self.subscription_type}")
                if self.bitcoin_address:
                    print(f"   • Bitcoin Address: {self.bitcoin_address}")
                print(f"   • Samples Collected: {len(self.samples_collected)}")
                print("   • Connection Status: ✅ SUCCESS")
                
                ws.close()
                
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing JSON message: {e}")
            print(f"Raw message: {message}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
            
    def on_error(self, ws, error):
        """Called when an error occurs."""
        print(f"❌ WebSocket Error: {error}")
        
    def on_close(self, ws, close_status_code, close_msg):
        """Called when WebSocket connection is closed."""
        print(f"\n🔌 WebSocket connection closed")
        if close_status_code:
            print(f"   Status Code: {close_status_code}")
        if close_msg:
            print(f"   Message: {close_msg}")
            
        if self.connection_established:
            print("✅ Connection test completed successfully!")
        else:
            print("❌ Connection test failed - could not establish connection")
            
    def test_connection(self, timeout: int = 30):
        """
        Test the WebSocket connection and collect sample data.
        
        Args:
            timeout: Maximum time to wait for samples (seconds)
        """
        print("🚀 Starting Blockchain.com WebSocket Connection Test")
        print(f"Target: {self.ws_url}")
        print(f"Subscription: {self.subscription_type}")
        if self.bitcoin_address:
            print(f"Bitcoin Address: {self.bitcoin_address}")
        print(f"Collecting {self.max_samples} sample transactions...\n")
        
        try:
            # Configure WebSocket
            ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            # Run WebSocket with timeout
            ws.run_forever()
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            print("\n🔍 Troubleshooting Tips:")
            print("   • Check internet connection")
            print("   • Verify WebSocket URL is correct")
            print("   • Ensure firewall allows WebSocket connections")
            return False
            
        return True


def main():
    """Main function to run the blockchain.com WebSocket connection test."""
    
    # Load configuration from environment variables
    ws_url = os.environ.get("BLOCKCHAIN_WS_URL", "wss://ws.blockchain.info/inv")
    subscription_type = os.environ.get("SUBSCRIPTION_TYPE", "unconfirmed_sub")
    bitcoin_address = os.environ.get("BITCOIN_ADDRESS", "").strip()
    
    # Validate configuration
    if subscription_type == "addr_sub" and not bitcoin_address:
        print("❌ Error: BITCOIN_ADDRESS is required when SUBSCRIPTION_TYPE is 'addr_sub'")
        print("Please set the BITCOIN_ADDRESS environment variable to a valid Bitcoin address.")
        return
        
    if subscription_type not in ["unconfirmed_sub", "addr_sub"]:
        print(f"❌ Error: Invalid SUBSCRIPTION_TYPE '{subscription_type}'")
        print("Valid options: 'unconfirmed_sub' or 'addr_sub'")
        return
    
    # Create and run the tester
    tester = BlockchainWebSocketTester(
        ws_url=ws_url,
        subscription_type=subscription_type,
        bitcoin_address=bitcoin_address if bitcoin_address else None
    )
    
    # Run the connection test
    success = tester.test_connection(timeout=30)
    
    if success and len(tester.samples_collected) > 0:
        print("\n🎯 Connection Test Results:")
        print("   ✅ Successfully connected to blockchain.com WebSocket")
        print("   ✅ Successfully subscribed to Bitcoin transaction stream")
        print("   ✅ Successfully received and parsed transaction data")
        print(f"   ✅ Collected {len(tester.samples_collected)} sample transactions")
        print("\n📋 Sample Data Structure Analysis:")
        
        if tester.samples_collected:
            sample = tester.samples_collected[0]
            if isinstance(sample, dict):
                print("   Key fields found in transaction data:")
                for key in sorted(sample.keys()):
                    value = sample[key]
                    value_type = type(value).__name__
                    if isinstance(value, list):
                        value_info = f"list with {len(value)} items"
                    elif isinstance(value, dict):
                        value_info = f"dict with keys: {list(value.keys())[:3]}..."
                    else:
                        value_info = f"{value_type}: {str(value)[:50]}..."
                    print(f"     • {key}: {value_info}")
    else:
        print("\n❌ Connection test failed or no data received")
        print("Please check the error messages above for troubleshooting information")


#  Sources require execution under a conditional main
if __name__ == "__main__":
    main()