import os
import json
import websocket
import time
import sys
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


def test_blockchain_connection():
    """
    Test connection to blockchain.com websocket and retrieve sample Bitcoin transaction data.
    This is a connection test only - no Kafka integration.
    """
    
    # Get configuration from environment variables
    websocket_url = os.getenv('WEBSOCKET_URL', 'wss://ws.blockchain.info/inv')
    subscription_type = os.getenv('SUBSCRIPTION_TYPE', 'unconfirmed_sub')
    bitcoin_address = os.getenv('BITCOIN_ADDRESS', '')
    
    print("=" * 60)
    print("BLOCKCHAIN.COM WEBSOCKET CONNECTION TEST")
    print("=" * 60)
    print(f"WebSocket URL: {websocket_url}")
    print(f"Subscription Type: {subscription_type}")
    if bitcoin_address and subscription_type == 'addr_sub':
        print(f"Bitcoin Address: {bitcoin_address}")
    print(f"Started at: {datetime.now()}")
    print("-" * 60)
    
    # Counter for received messages
    received_count = 0
    target_count = 10
    
    def on_message(ws, message):
        """Handle incoming websocket messages"""
        nonlocal received_count
        
        try:
            data = json.loads(message)
            received_count += 1
            
            print(f"\n📦 TRANSACTION #{received_count}")
            print(f"Timestamp: {datetime.now()}")
            
            if data.get('op') == 'utx':
                # Unconfirmed transaction
                tx_data = data.get('x', {})
                print(f"Operation: {data.get('op')} (Unconfirmed Transaction)")
                print(f"Transaction Hash: {tx_data.get('hash', 'N/A')}")
                print(f"Transaction Size: {tx_data.get('size', 'N/A')} bytes")
                print(f"Input Count: {tx_data.get('vin_sz', 'N/A')}")
                print(f"Output Count: {tx_data.get('vout_sz', 'N/A')}")
                print(f"Transaction Time: {tx_data.get('time', 'N/A')}")
                
                # Show transaction outputs (recipients)
                outputs = tx_data.get('out', [])
                if outputs:
                    print("Outputs:")
                    for i, output in enumerate(outputs[:3]):  # Show first 3 outputs
                        value_btc = output.get('value', 0) / 100000000  # Convert satoshis to BTC
                        addr = output.get('addr', 'N/A')
                        print(f"  [{i+1}] {addr}: {value_btc:.8f} BTC")
                
                # Show transaction inputs (senders)
                inputs = tx_data.get('inputs', [])
                if inputs:
                    print("Inputs:")
                    for i, inp in enumerate(inputs[:3]):  # Show first 3 inputs
                        prev_out = inp.get('prev_out', {})
                        value_btc = prev_out.get('value', 0) / 100000000  # Convert satoshis to BTC
                        addr = prev_out.get('addr', 'N/A')
                        print(f"  [{i+1}] {addr}: {value_btc:.8f} BTC")
                        
            elif data.get('op') == 'block':
                # New block
                block_data = data.get('x', {})
                print(f"Operation: {data.get('op')} (New Block)")
                print(f"Block Hash: {block_data.get('hash', 'N/A')}")
                print(f"Block Height: {block_data.get('height', 'N/A')}")
                print(f"Block Size: {block_data.get('size', 'N/A')} bytes")
                print(f"Transaction Count: {block_data.get('nTx', 'N/A')}")
                print(f"Block Time: {block_data.get('time', 'N/A')}")
                
            else:
                print(f"Operation: {data.get('op', 'Unknown')}")
                
            # Print raw JSON for inspection (truncated for readability)
            json_str = json.dumps(data, indent=2)
            if len(json_str) > 1000:
                json_str = json_str[:1000] + "...\n}"
            print(f"Raw JSON: {json_str}")
            
            # Stop after receiving target number of messages
            if received_count >= target_count:
                print(f"\n✅ Successfully received {target_count} sample transactions!")
                print("Connection test completed successfully.")
                ws.close()
                
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing JSON: {e}")
            print(f"Raw message: {message}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def on_error(ws, error):
        """Handle websocket errors"""
        print(f"❌ WebSocket Error: {error}")
    
    def on_close(ws, close_status_code, close_msg):
        """Handle websocket close"""
        print(f"\n🔌 WebSocket connection closed.")
        if close_status_code:
            print(f"Close status: {close_status_code}")
        if close_msg:
            print(f"Close message: {close_msg}")
        print(f"Total messages received: {received_count}")
    
    def on_open(ws):
        """Handle websocket connection open"""
        print("✅ WebSocket connection established!")
        
        try:
            # Subscribe based on configuration
            if subscription_type == 'addr_sub' and bitcoin_address:
                # Subscribe to specific Bitcoin address
                subscription_msg = {
                    "op": "addr_sub",
                    "addr": bitcoin_address
                }
                print(f"📡 Subscribing to address: {bitcoin_address}")
            else:
                # Subscribe to all unconfirmed transactions (default)
                subscription_msg = {
                    "op": "unconfirmed_sub"
                }
                print("📡 Subscribing to all unconfirmed transactions...")
            
            ws.send(json.dumps(subscription_msg))
            print("📡 Subscription message sent successfully!")
            print(f"⏳ Waiting for {target_count} sample transactions...")
            
        except Exception as e:
            print(f"❌ Error sending subscription: {e}")
            ws.close()
    
    try:
        # Create WebSocket connection
        print("🔗 Connecting to blockchain.com websocket...")
        ws = websocket.WebSocketApp(
            websocket_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        # Run with timeout to avoid hanging indefinitely
        ws.run_forever(ping_interval=30, ping_timeout=10)
        
    except KeyboardInterrupt:
        print("\n⚠️ Connection test interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("- Check your internet connection")
        print("- Verify the WebSocket URL is correct")
        print("- Ensure no firewall is blocking the connection")
        sys.exit(1)


def main():
    """Main entry point for connection testing"""
    
    # Check if this is a connection test (no Kafka integration)
    print("🧪 Running Bitcoin blockchain connection test...")
    print("Note: This is a connection test only - no Kafka integration")
    
    try:
        test_blockchain_connection()
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()