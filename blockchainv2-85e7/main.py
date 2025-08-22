"""
Bitcoin Transaction Data Connection Test

This script tests the connection to the blockchain.com WebSocket API
to read real-time Bitcoin transaction data. This is a connection test only
- no Kafka/Quix integration yet.
"""
import asyncio
import json
import os
import sys
import websockets
from datetime import datetime
from typing import Optional

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BlockchainWebSocketTester:
    """Test connection to blockchain.com WebSocket API for Bitcoin transaction data"""
    
    def __init__(self):
        self.websocket_url = os.environ.get("BLOCKCHAIN_WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
        self.subscription_type = os.environ.get("SUBSCRIPTION_TYPE", "unconfirmed_sub")
        self.bitcoin_address = os.environ.get("BITCOIN_ADDRESS", "")
        self.connection_timeout = int(os.environ.get("CONNECTION_TIMEOUT", "30"))
        self.max_messages = int(os.environ.get("MAX_MESSAGES", "10"))
        
        self.messages_received = 0
        self.connection_successful = False
        
    def print_connection_info(self):
        """Print connection configuration"""
        print("=" * 60)
        print("BLOCKCHAIN.COM WEBSOCKET CONNECTION TEST")
        print("=" * 60)
        print(f"WebSocket URL: {self.websocket_url}")
        print(f"Subscription Type: {self.subscription_type}")
        if self.bitcoin_address and self.subscription_type == "addr_sub":
            print(f"Bitcoin Address: {self.bitcoin_address}")
        print(f"Connection Timeout: {self.connection_timeout}s")
        print(f"Max Messages to Receive: {self.max_messages}")
        print(f"Test Start Time: {datetime.now().isoformat()}")
        print("-" * 60)
        
    def get_subscription_message(self) -> dict:
        """Get the appropriate subscription message based on subscription type"""
        if self.subscription_type == "unconfirmed_sub":
            return {"op": "unconfirmed_sub"}
        elif self.subscription_type == "addr_sub":
            if not self.bitcoin_address:
                raise ValueError("Bitcoin address is required for addr_sub subscription type")
            return {"op": "addr_sub", "addr": self.bitcoin_address}
        elif self.subscription_type == "blocks_sub":
            return {"op": "blocks_sub"}
        else:
            raise ValueError(f"Unsupported subscription type: {self.subscription_type}")
    
    def format_transaction_data(self, data: dict) -> str:
        """Format transaction data for readable output"""
        if data.get("op") == "utx" and "x" in data:
            tx = data["x"]
            formatted = f"""
TRANSACTION DATA:
  Hash: {tx.get('hash', 'N/A')}
  Size: {tx.get('size', 'N/A')} bytes
  Inputs: {tx.get('vin_sz', 'N/A')}
  Outputs: {tx.get('vout_sz', 'N/A')}
  Time: {datetime.fromtimestamp(tx.get('time', 0)).isoformat() if tx.get('time') else 'N/A'}
  Lock Time: {tx.get('lock_time', 'N/A')}
  Version: {tx.get('ver', 'N/A')}
  
  OUTPUT ADDRESSES & VALUES:"""
            
            if "out" in tx:
                for i, output in enumerate(tx["out"]):
                    value_btc = output.get("value", 0) / 100000000  # Convert satoshis to BTC
                    formatted += f"""
    Output {i}: {output.get('addr', 'N/A')} - {value_btc:.8f} BTC"""
            
            return formatted
        
        elif data.get("op") == "block" and "x" in data:
            block = data["x"]
            formatted = f"""
BLOCK DATA:
  Hash: {block.get('hash', 'N/A')}
  Height: {block.get('height', 'N/A')}
  Size: {block.get('size', 'N/A')} bytes
  Number of Transactions: {block.get('nTx', 'N/A')}
  Time: {datetime.fromtimestamp(block.get('time', 0)).isoformat() if block.get('time') else 'N/A'}
  Merkle Root: {block.get('mrklRoot', 'N/A')}
  Previous Block Index: {block.get('prevBlockIndex', 'N/A')}
  Total BTC Sent: {block.get('totalBTCSent', 'N/A')}
  Estimated BTC Sent: {block.get('estimatedBTCSent', 'N/A')}"""
            return formatted
        
        return f"Raw data: {json.dumps(data, indent=2)}"
    
    async def test_connection(self):
        """Test WebSocket connection to blockchain.com"""
        try:
            print("Attempting to connect to blockchain.com WebSocket...")
            
            async with websockets.connect(
                self.websocket_url,
                timeout=self.connection_timeout
            ) as websocket:
                
                print("✓ Connection established successfully!")
                self.connection_successful = True
                
                # Subscribe to the desired data stream
                subscription_msg = self.get_subscription_message()
                await websocket.send(json.dumps(subscription_msg))
                print(f"✓ Subscription message sent: {subscription_msg}")
                
                print(f"\nWaiting for {self.max_messages} messages...")
                print("=" * 60)
                
                # Listen for messages
                while self.messages_received < self.max_messages:
                    try:
                        message = await asyncio.wait_for(
                            websocket.recv(), 
                            timeout=self.connection_timeout
                        )
                        
                        data = json.loads(message)
                        self.messages_received += 1
                        
                        print(f"\nMESSAGE {self.messages_received}:")
                        print(f"Received at: {datetime.now().isoformat()}")
                        print(self.format_transaction_data(data))
                        print("-" * 60)
                        
                    except asyncio.TimeoutError:
                        print(f"⚠ Timeout waiting for message after {self.connection_timeout}s")
                        break
                    except json.JSONDecodeError as e:
                        print(f"⚠ Failed to decode JSON message: {e}")
                        print(f"Raw message: {message}")
                        continue
                        
        except websockets.exceptions.ConnectionClosed as e:
            print(f"✗ WebSocket connection closed: {e}")
            return False
        except websockets.exceptions.InvalidURI as e:
            print(f"✗ Invalid WebSocket URI: {e}")
            return False
        except websockets.exceptions.WebSocketException as e:
            print(f"✗ WebSocket error: {e}")
            return False
        except asyncio.TimeoutError:
            print(f"✗ Connection timeout after {self.connection_timeout}s")
            return False
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            return False
            
        return True
    
    def print_summary(self):
        """Print test summary"""
        print("\n" + "=" * 60)
        print("CONNECTION TEST SUMMARY")
        print("=" * 60)
        print(f"Connection Successful: {'✓ YES' if self.connection_successful else '✗ NO'}")
        print(f"Messages Received: {self.messages_received}")
        print(f"Subscription Type: {self.subscription_type}")
        print(f"Test End Time: {datetime.now().isoformat()}")
        
        if self.messages_received > 0:
            print(f"\n✓ Successfully received {self.messages_received} Bitcoin transaction/block messages")
            print("✓ Data structure validated and can be used for Kafka integration")
        else:
            print("\n⚠ No messages received - check subscription type and network connectivity")
        
        print("=" * 60)


async def main():
    """Main function to run the connection test"""
    try:
        tester = BlockchainWebSocketTester()
        tester.print_connection_info()
        
        success = await tester.test_connection()
        tester.print_summary()
        
        if success and tester.messages_received > 0:
            print("\n✓ Connection test completed successfully!")
            return 0
        else:
            print("\n✗ Connection test failed!")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠ Test interrupted by user")
        return 1
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)