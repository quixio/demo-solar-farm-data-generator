import asyncio
import json
import os
import websockets
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


async def test_blockchain_websocket_connection():
    """
    Test connection to blockchain.com WebSocket API to read bitcoin transaction data.
    This is a connection test only - no Kafka integration yet.
    """
    
    # Get configuration from environment variables
    websocket_url = os.environ.get("WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
    subscription_type = os.environ.get("SUBSCRIPTION_TYPE", "unconfirmed_sub")
    bitcoin_address = os.environ.get("BITCOIN_ADDRESS", "")
    
    print("=== Bitcoin Blockchain WebSocket Connection Test ===")
    print(f"WebSocket URL: {websocket_url}")
    print(f"Subscription Type: {subscription_type}")
    if bitcoin_address:
        print(f"Bitcoin Address: {bitcoin_address}")
    print("=" * 50)
    
    transaction_count = 0
    max_transactions = 10
    
    try:
        print(f"Connecting to {websocket_url}...")
        async with websockets.connect(websocket_url) as websocket:
            print("✓ Successfully connected to blockchain.com WebSocket!")
            
            # Prepare subscription message
            if subscription_type == "addr_sub" and bitcoin_address:
                subscription_msg = {
                    "op": "addr_sub",
                    "addr": bitcoin_address
                }
                print(f"Subscribing to address: {bitcoin_address}")
            elif subscription_type == "block_sub":
                subscription_msg = {
                    "op": "blocks_sub"
                }
                print("Subscribing to new blocks")
            else:
                subscription_msg = {
                    "op": "unconfirmed_sub"
                }
                print("Subscribing to unconfirmed transactions")
            
            # Send subscription message
            await websocket.send(json.dumps(subscription_msg))
            print("✓ Subscription message sent!")
            
            print(f"\nWaiting for {max_transactions} sample records...")
            print("-" * 50)
            
            # Listen for messages
            while transaction_count < max_transactions:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                    data = json.loads(message)
                    
                    transaction_count += 1
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    print(f"\n📊 RECORD #{transaction_count} [{timestamp}]")
                    print("=" * 60)
                    
                    if data.get("op") == "utx":
                        # Unconfirmed transaction
                        tx_data = data.get("x", {})
                        print(f"Transaction Type: Unconfirmed Transaction")
                        print(f"Hash: {tx_data.get('hash', 'N/A')}")
                        print(f"Size: {tx_data.get('size', 'N/A')} bytes")
                        print(f"Value: {sum(output.get('value', 0) for output in tx_data.get('out', []))} satoshis")
                        print(f"Inputs: {tx_data.get('vin_sz', 'N/A')}")
                        print(f"Outputs: {tx_data.get('vout_sz', 'N/A')}")
                        print(f"Time: {tx_data.get('time', 'N/A')}")
                        print(f"Relayed by: {tx_data.get('relayed_by', 'N/A')}")
                        
                        # Show first input and output for detailed inspection
                        inputs = tx_data.get('inputs', [])
                        if inputs:
                            first_input = inputs[0]
                            prev_out = first_input.get('prev_out', {})
                            print(f"First Input Address: {prev_out.get('addr', 'N/A')}")
                            print(f"First Input Value: {prev_out.get('value', 'N/A')} satoshis")
                        
                        outputs = tx_data.get('out', [])
                        if outputs:
                            first_output = outputs[0]
                            print(f"First Output Address: {first_output.get('addr', 'N/A')}")
                            print(f"First Output Value: {first_output.get('value', 'N/A')} satoshis")
                    
                    elif data.get("op") == "block":
                        # New block
                        block_data = data.get("x", {})
                        print(f"Transaction Type: New Block")
                        print(f"Hash: {block_data.get('hash', 'N/A')}")
                        print(f"Height: {block_data.get('height', 'N/A')}")
                        print(f"Size: {block_data.get('size', 'N/A')} bytes")
                        print(f"Transaction Count: {block_data.get('nTx', 'N/A')}")
                        print(f"Total BTC Sent: {block_data.get('totalBTCSent', 'N/A')} satoshis")
                        print(f"Time: {block_data.get('time', 'N/A')}")
                        print(f"Merkle Root: {block_data.get('mrklRoot', 'N/A')}")
                    
                    else:
                        # Other message types
                        print(f"Message Type: {data.get('op', 'unknown')}")
                        print(f"Raw Data: {json.dumps(data, indent=2)}")
                    
                    print("=" * 60)
                
                except asyncio.TimeoutError:
                    print(f"⚠️ Timeout waiting for message after 30 seconds")
                    break
                except json.JSONDecodeError as e:
                    print(f"❌ Error parsing JSON: {e}")
                    print(f"Raw message: {message}")
                    continue
                except Exception as e:
                    print(f"❌ Error processing message: {e}")
                    continue
            
            print(f"\n✅ Successfully received {transaction_count} records!")
            
            # Show connection metadata
            print("\n📋 CONNECTION METADATA:")
            print(f"   • WebSocket URL: {websocket_url}")
            print(f"   • Subscription Type: {subscription_type}")
            print(f"   • Records Received: {transaction_count}/{max_transactions}")
            print(f"   • Connection Status: Active")
            print(f"   • API Rate Limit: No authentication required")
            print(f"   • Data Format: JSON over WebSocket")
            
    except websockets.exceptions.ConnectionClosed as e:
        print(f"❌ WebSocket connection closed: {e}")
        return False
    except websockets.exceptions.WebSocketException as e:
        print(f"❌ WebSocket error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
    
    return True


def main():
    """
    Main function to run the blockchain.com WebSocket connection test.
    This is a connection test only - no Quix Streams or Kafka integration yet.
    """
    
    print("Starting blockchain.com WebSocket connection test...")
    
    try:
        # Run the async connection test
        success = asyncio.run(test_blockchain_websocket_connection())
        
        if success:
            print("\n🎉 CONNECTION TEST SUCCESSFUL!")
            print("Ready to integrate with Quix Streams in the next step.")
        else:
            print("\n❌ CONNECTION TEST FAILED!")
            print("Please check your network connection and configuration.")
            
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")


if __name__ == "__main__":
    main()