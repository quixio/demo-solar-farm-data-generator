import os
import json
import time
import websocket
from datetime import datetime
import threading

def test_blockchain_websocket_connection():
    """
    Test connection to blockchain.com WebSocket API to read bitcoin transaction data.
    This is a connection test only - no Kafka integration yet.
    """
    
    # Get configuration from environment variables
    websocket_url = os.environ.get("WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
    subscription_type = os.environ.get("SUBSCRIPTION_TYPE", "unconfirmed_sub")
    bitcoin_address = os.environ.get("BITCOIN_ADDRESS", "")
    max_messages = int(os.environ.get("MAX_MESSAGES", "10"))
    
    print(f"=== Blockchain.com WebSocket Connection Test ===")
    print(f"WebSocket URL: {websocket_url}")
    print(f"Subscription Type: {subscription_type}")
    if bitcoin_address:
        print(f"Bitcoin Address: {bitcoin_address}")
    print(f"Max Messages to Read: {max_messages}")
    print(f"Test Started: {datetime.now()}\n")
    
    # Counter for received messages
    message_count = 0
    connection_successful = False
    
    def on_message(ws, message):
        nonlocal message_count, max_messages
        
        try:
            # Parse the JSON message
            data = json.loads(message)
            message_count += 1
            
            print(f"\n--- Message {message_count} ---")
            print(f"Timestamp: {datetime.now()}")
            print(f"Operation: {data.get('op', 'unknown')}")
            
            # Format the message based on type
            if data.get('op') == 'utx':  # Unconfirmed transaction
                tx_data = data.get('x', {})
                print(f"Transaction Hash: {tx_data.get('hash', 'N/A')}")
                print(f"Transaction Size: {tx_data.get('size', 'N/A')} bytes")
                print(f"Transaction Time: {tx_data.get('time', 'N/A')}")
                print(f"Input Count: {tx_data.get('vin_sz', 'N/A')}")
                print(f"Output Count: {tx_data.get('vout_sz', 'N/A')}")
                
                # Show input and output details
                inputs = tx_data.get('inputs', [])
                outputs = tx_data.get('out', [])
                
                if inputs:
                    print(f"First Input Address: {inputs[0].get('prev_out', {}).get('addr', 'N/A')}")
                    print(f"First Input Value: {inputs[0].get('prev_out', {}).get('value', 'N/A')} satoshi")
                
                if outputs:
                    print(f"First Output Address: {outputs[0].get('addr', 'N/A')}")
                    print(f"First Output Value: {outputs[0].get('value', 'N/A')} satoshi")
                    
            elif data.get('op') == 'block':  # New block
                block_data = data.get('x', {})
                print(f"Block Hash: {block_data.get('hash', 'N/A')}")
                print(f"Block Height: {block_data.get('height', 'N/A')}")
                print(f"Block Size: {block_data.get('size', 'N/A')} bytes")
                print(f"Transaction Count: {block_data.get('nTx', 'N/A')}")
                print(f"Block Time: {block_data.get('time', 'N/A')}")
                
            else:
                # For other message types, show the raw data
                print("Raw Message Data:")
                print(json.dumps(data, indent=2))
            
            print("-" * 50)
            
            # Stop after receiving the specified number of messages
            if message_count >= max_messages:
                print(f"\n=== Test Completed Successfully ===")
                print(f"Total Messages Received: {message_count}")
                print(f"Connection Status: SUCCESS")
                print(f"Test Ended: {datetime.now()}")
                ws.close()
                
        except json.JSONDecodeError as e:
            print(f"Error parsing message: {e}")
            print(f"Raw message: {message}")
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def on_error(ws, error):
        print(f"\n❌ WebSocket Error: {error}")
    
    def on_close(ws, close_status_code, close_msg):
        print(f"\n=== WebSocket Connection Closed ===")
        if close_status_code:
            print(f"Close Status Code: {close_status_code}")
        if close_msg:
            print(f"Close Message: {close_msg}")
    
    def on_open(ws):
        nonlocal connection_successful
        connection_successful = True
        print("✅ WebSocket Connection Established Successfully!")
        
        # Subscribe to the specified channel
        try:
            if subscription_type == "addr_sub" and bitcoin_address:
                # Subscribe to specific address
                subscription_message = {
                    "op": "addr_sub",
                    "addr": bitcoin_address
                }
                print(f"Subscribing to address: {bitcoin_address}")
            elif subscription_type == "unconfirmed_sub":
                # Subscribe to all unconfirmed transactions
                subscription_message = {
                    "op": "unconfirmed_sub"
                }
                print("Subscribing to all unconfirmed transactions...")
            elif subscription_type == "blocks_sub":
                # Subscribe to new blocks
                subscription_message = {
                    "op": "blocks_sub"
                }
                print("Subscribing to new blocks...")
            else:
                print(f"❌ Unknown subscription type: {subscription_type}")
                ws.close()
                return
            
            # Send subscription message
            ws.send(json.dumps(subscription_message))
            print("✅ Subscription message sent successfully!")
            print("Waiting for messages...\n")
            
        except Exception as e:
            print(f"❌ Error sending subscription: {e}")
            ws.close()
    
    try:
        # Create WebSocket connection
        print("🔄 Attempting to connect to blockchain.com WebSocket...")
        
        # Enable WebSocket debug traces for troubleshooting
        websocket.enableTrace(False)  # Set to True for detailed debugging
        
        ws = websocket.WebSocketApp(
            websocket_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        # Set a timeout for the connection
        def timeout_handler():
            time.sleep(30)  # 30 second timeout
            if message_count == 0:
                print("\n⏱️ Timeout: No messages received within 30 seconds")
                print("This might be normal if there are no new transactions")
                ws.close()
        
        timeout_thread = threading.Thread(target=timeout_handler)
        timeout_thread.daemon = True
        timeout_thread.start()
        
        # Run the WebSocket connection
        ws.run_forever()
        
        if not connection_successful:
            print("\n❌ Failed to establish WebSocket connection")
            return False
            
        return message_count > 0
        
    except Exception as e:
        print(f"\n❌ Connection Error: {e}")
        print("Please check your internet connection and try again.")
        return False

def main():
    """
    Main function for testing blockchain.com WebSocket connection.
    This is a connection test only - no Quix Streams integration yet.
    """
    print("Starting blockchain.com WebSocket connection test...")
    
    try:
        success = test_blockchain_websocket_connection()
        
        if success:
            print("\n🎉 Connection test completed successfully!")
            print("The blockchain.com WebSocket API is accessible and returning data.")
        else:
            print("\n❌ Connection test failed!")
            print("Please check the configuration and try again.")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error during test: {e}")

if __name__ == "__main__":
    main()