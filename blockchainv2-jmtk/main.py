"""
Blockchain.com WebSocket Connection Test Script

This script tests the connection to Blockchain.com's WebSocket API to receive
real-time Bitcoin transaction data. It's designed to:
1. Connect to the WebSocket endpoint
2. Subscribe to transaction feeds (all transactions or specific address)
3. Receive and display exactly 10 sample transactions
4. Provide clear output for data structure analysis

This is a CONNECTION TEST ONLY - no Kafka integration yet.
"""

import asyncio
import json
import os
import websockets
import sys
from datetime import datetime
from typing import Dict, Any, Optional

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BlockchainWebSocketTester:
    """
    Test connection to Blockchain.com WebSocket API for Bitcoin transaction data.
    """
    
    def __init__(self):
        self.websocket_url = os.environ.get("WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
        self.subscription_type = os.environ.get("SUBSCRIPTION_TYPE", "unconfirmed_sub")
        self.bitcoin_address = os.environ.get("BITCOIN_ADDRESS", "")
        self.max_messages = int(os.environ.get("MAX_MESSAGES", "10"))
        self.message_count = 0
        
        print("=" * 60)
        print("BLOCKCHAIN.COM WEBSOCKET CONNECTION TEST")
        print("=" * 60)
        print(f"WebSocket URL: {self.websocket_url}")
        print(f"Subscription Type: {self.subscription_type}")
        if self.bitcoin_address and self.subscription_type == "addr_sub":
            print(f"Bitcoin Address: {self.bitcoin_address}")
        print(f"Max Messages to Receive: {self.max_messages}")
        print("=" * 60)
    
    async def connect_and_test(self):
        """
        Main method to connect to WebSocket and test data reception.
        """
        try:
            print(f"[{self._get_timestamp()}] Connecting to {self.websocket_url}...")
            
            async with websockets.connect(
                self.websocket_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            ) as websocket:
                print(f"[{self._get_timestamp()}] ✓ WebSocket connection established!")
                
                # Subscribe to the appropriate feed
                await self._subscribe(websocket)
                
                # Listen for messages
                await self._listen_for_messages(websocket)
                
        except websockets.exceptions.ConnectionClosed as e:
            print(f"[{self._get_timestamp()}] ✗ WebSocket connection closed: {e}")
        except websockets.exceptions.WebSocketException as e:
            print(f"[{self._get_timestamp()}] ✗ WebSocket error: {e}")
        except Exception as e:
            print(f"[{self._get_timestamp()}] ✗ Unexpected error: {e}")
            sys.exit(1)
    
    async def _subscribe(self, websocket):
        """
        Subscribe to the appropriate blockchain data feed.
        """
        if self.subscription_type == "addr_sub":
            if not self.bitcoin_address:
                raise ValueError("BITCOIN_ADDRESS must be provided when using addr_sub subscription type")
            
            subscription_message = {
                "op": "addr_sub",
                "addr": self.bitcoin_address
            }
            print(f"[{self._get_timestamp()}] Subscribing to address: {self.bitcoin_address}")
            
        elif self.subscription_type == "unconfirmed_sub":
            subscription_message = {
                "op": "unconfirmed_sub"
            }
            print(f"[{self._get_timestamp()}] Subscribing to all unconfirmed transactions...")
            
        else:
            raise ValueError(f"Unsupported subscription type: {self.subscription_type}")
        
        await websocket.send(json.dumps(subscription_message))
        print(f"[{self._get_timestamp()}] ✓ Subscription message sent!")
        print(f"[{self._get_timestamp()}] Waiting for transaction data...")
        print()
    
    async def _listen_for_messages(self, websocket):
        """
        Listen for incoming messages and process them.
        """
        try:
            async for message in websocket:
                if self.message_count >= self.max_messages:
                    print(f"[{self._get_timestamp()}] ✓ Received {self.max_messages} messages. Test complete!")
                    break
                
                await self._process_message(message)
                
        except websockets.exceptions.ConnectionClosed:
            print(f"[{self._get_timestamp()}] Connection closed by server")
        except Exception as e:
            print(f"[{self._get_timestamp()}] Error listening for messages: {e}")
    
    async def _process_message(self, message: str):
        """
        Process and display incoming transaction messages.
        """
        try:
            data = json.loads(message)
            self.message_count += 1
            
            print(f"📦 MESSAGE #{self.message_count}")
            print(f"⏰ Received at: {self._get_timestamp()}")
            print("-" * 50)
            
            if data.get("op") == "utx":
                # This is an unconfirmed transaction
                self._display_transaction(data["x"])
            elif data.get("op") == "block":
                # This is a new block
                self._display_block(data["x"])
            else:
                # Unknown message type
                print("Raw message:")
                print(json.dumps(data, indent=2))
            
            print("-" * 50)
            print()
            
        except json.JSONDecodeError as e:
            print(f"[{self._get_timestamp()}] ✗ Failed to parse JSON message: {e}")
            print(f"Raw message: {message}")
        except Exception as e:
            print(f"[{self._get_timestamp()}] ✗ Error processing message: {e}")
    
    def _display_transaction(self, transaction: Dict[str, Any]):
        """
        Display transaction data in a readable format.
        """
        print("🔗 BITCOIN TRANSACTION")
        print(f"Hash: {transaction.get('hash', 'N/A')}")
        print(f"Size: {transaction.get('size', 'N/A')} bytes")
        print(f"Time: {self._format_timestamp(transaction.get('time'))}")
        print(f"Inputs: {transaction.get('vin_sz', 0)}")
        print(f"Outputs: {transaction.get('vout_sz', 0)}")
        print(f"Lock Time: {transaction.get('lock_time', 'N/A')}")
        print(f"Relayed By: {transaction.get('relayed_by', 'N/A')}")
        
        # Display input details
        if transaction.get('inputs'):
            print("\n💰 INPUTS:")
            for i, inp in enumerate(transaction['inputs'][:2]):  # Show first 2 inputs
                if 'prev_out' in inp:
                    prev_out = inp['prev_out']
                    value_btc = prev_out.get('value', 0) / 100000000  # Convert satoshis to BTC
                    print(f"  Input {i+1}: {prev_out.get('addr', 'N/A')} ({value_btc:.8f} BTC)")
        
        # Display output details
        if transaction.get('out'):
            print("\n💸 OUTPUTS:")
            for i, out in enumerate(transaction['out'][:2]):  # Show first 2 outputs
                value_btc = out.get('value', 0) / 100000000  # Convert satoshis to BTC
                print(f"  Output {i+1}: {out.get('addr', 'N/A')} ({value_btc:.8f} BTC)")
    
    def _display_block(self, block: Dict[str, Any]):
        """
        Display block data in a readable format.
        """
        print("📦 NEW BLOCK")
        print(f"Hash: {block.get('hash', 'N/A')}")
        print(f"Height: {block.get('height', 'N/A')}")
        print(f"Time: {self._format_timestamp(block.get('time'))}")
        print(f"Transactions: {block.get('nTx', 'N/A')}")
        print(f"Size: {block.get('size', 'N/A')} bytes")
        print(f"Total BTC Sent: {block.get('totalBTCSent', 0) / 100000000:.8f} BTC")
    
    def _get_timestamp(self) -> str:
        """
        Get current timestamp as formatted string.
        """
        return datetime.now().strftime("%H:%M:%S")
    
    def _format_timestamp(self, unix_timestamp: Optional[int]) -> str:
        """
        Format Unix timestamp to readable format.
        """
        if unix_timestamp:
            return datetime.fromtimestamp(unix_timestamp).strftime("%Y-%m-%d %H:%M:%S")
        return "N/A"


async def main():
    """
    Main function to run the blockchain WebSocket test.
    """
    tester = BlockchainWebSocketTester()
    
    try:
        await tester.connect_and_test()
        print("\n" + "=" * 60)
        print("✓ CONNECTION TEST COMPLETED SUCCESSFULLY!")
        print("✓ Data structure analysis can proceed with the samples above")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print(f"\n[{tester._get_timestamp()}] Test interrupted by user")
    except Exception as e:
        print(f"\n[{tester._get_timestamp()}] Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())