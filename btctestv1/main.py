"""
Bitcoin Transaction Data Connection Test

This script tests the connection to blockchain.com WebSocket API to read live Bitcoin 
transaction data. This is a connection test only - no Kafka integration yet.

The script will:
1. Connect to blockchain.com WebSocket API
2. Subscribe to unconfirmed Bitcoin transactions
3. Collect exactly 10 sample transactions
4. Display formatted transaction data for inspection
5. Handle connection errors gracefully
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BitcoinTransactionTester:
    """
    Connection tester for blockchain.com WebSocket API to fetch Bitcoin transaction data.
    
    This class handles:
    - WebSocket connection to blockchain.com
    - Subscription management for unconfirmed transactions
    - Data collection and formatting
    - Error handling and retries
    """
    
    def __init__(self, max_transactions: int = 10):
        """
        Initialize the Bitcoin transaction tester.
        
        Args:
            max_transactions: Maximum number of transactions to collect for testing
        """
        self.websocket_url = "wss://ws.blockchain.info/inv"
        self.max_transactions = max_transactions
        self.collected_transactions: List[Dict] = []
        self.api_key = os.environ.get("API_KEY")  # Optional API key
        
    async def connect_and_test(self) -> bool:
        """
        Main method to connect to blockchain.com and test data retrieval.
        
        Returns:
            bool: True if connection test succeeded, False otherwise
        """
        print("=" * 60)
        print("🔗 Bitcoin Transaction Data Connection Test")
        print("=" * 60)
        print(f"📡 Connecting to: {self.websocket_url}")
        print(f"🎯 Target sample size: {self.max_transactions} transactions")
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(1, max_retries + 1):
            try:
                print(f"\n🔄 Connection attempt {attempt}/{max_retries}...")
                success = await self._attempt_connection()
                
                if success:
                    print("\n✅ Connection test completed successfully!")
                    self._print_summary()
                    return True
                    
            except Exception as e:
                print(f"❌ Attempt {attempt} failed: {str(e)}")
                
                if attempt < max_retries:
                    print(f"⏳ Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("🚫 All connection attempts failed!")
                    self._print_troubleshooting_info()
                    
        return False
    
    async def _attempt_connection(self) -> bool:
        """
        Attempt to connect to blockchain.com WebSocket and collect sample data.
        
        Returns:
            bool: True if data collection succeeded
        """
        timeout_seconds = 60  # Maximum wait time for collecting samples
        
        try:
            # Connect to WebSocket with timeout
            async with websockets.connect(
                self.websocket_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            ) as websocket:
                
                print("✅ WebSocket connection established")
                
                # Subscribe to unconfirmed transactions
                subscribe_message = {"op": "unconfirmed_sub"}
                await websocket.send(json.dumps(subscribe_message))
                print("📋 Subscribed to unconfirmed transactions")
                
                # Collect sample transactions
                start_time = time.time()
                
                while len(self.collected_transactions) < self.max_transactions:
                    # Check for timeout
                    if time.time() - start_time > timeout_seconds:
                        print(f"⏰ Timeout after {timeout_seconds} seconds")
                        break
                        
                    try:
                        # Wait for message with timeout
                        message = await asyncio.wait_for(
                            websocket.recv(), 
                            timeout=10.0
                        )
                        
                        await self._process_message(message)
                        
                    except asyncio.TimeoutError:
                        print("⏳ No new transactions in last 10 seconds, continuing to wait...")
                        continue
                
                # Unsubscribe before closing
                unsubscribe_message = {"op": "unconfirmed_unsub"}
                await websocket.send(json.dumps(unsubscribe_message))
                print("📋 Unsubscribed from transactions")
                
                return len(self.collected_transactions) > 0
                
        except ConnectionClosed:
            print("❌ WebSocket connection closed unexpectedly")
            return False
        except WebSocketException as e:
            print(f"❌ WebSocket error: {str(e)}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            return False
    
    async def _process_message(self, message: str) -> None:
        """
        Process incoming WebSocket message and extract transaction data.
        
        Args:
            message: Raw message from WebSocket
        """
        try:
            data = json.loads(message)
            
            # Check if this is a transaction message
            if data.get("op") == "utx" and "x" in data:
                transaction = data["x"]
                
                # Extract and format transaction data
                formatted_tx = self._format_transaction(transaction)
                self.collected_transactions.append(formatted_tx)
                
                tx_count = len(self.collected_transactions)
                print(f"📥 Transaction {tx_count}/{self.max_transactions} collected: {formatted_tx['hash'][:16]}...")
                
                # Print detailed info for first few transactions
                if tx_count <= 3:
                    self._print_transaction_details(formatted_tx)
                    
        except json.JSONDecodeError:
            print("⚠️ Received invalid JSON message, skipping...")
        except Exception as e:
            print(f"⚠️ Error processing message: {str(e)}")
    
    def _format_transaction(self, raw_tx: Dict) -> Dict:
        """
        Format raw transaction data into a structured format.
        
        Args:
            raw_tx: Raw transaction data from blockchain.com
            
        Returns:
            Dict: Formatted transaction data
        """
        return {
            "hash": raw_tx.get("hash", ""),
            "time": raw_tx.get("time", 0),
            "timestamp": datetime.fromtimestamp(raw_tx.get("time", 0)).isoformat() if raw_tx.get("time") else "",
            "size": raw_tx.get("size", 0),
            "fee": raw_tx.get("fee", 0),
            "input_count": raw_tx.get("vin_sz", 0),
            "output_count": raw_tx.get("vout_sz", 0),
            "total_input_value": sum(
                inp.get("prev_out", {}).get("value", 0) 
                for inp in raw_tx.get("inputs", [])
            ),
            "total_output_value": sum(
                out.get("value", 0) 
                for out in raw_tx.get("out", [])
            ),
            "addresses": {
                "inputs": [
                    inp.get("prev_out", {}).get("addr")
                    for inp in raw_tx.get("inputs", [])
                    if inp.get("prev_out", {}).get("addr")
                ],
                "outputs": [
                    out.get("addr")
                    for out in raw_tx.get("out", [])
                    if out.get("addr")
                ]
            },
            "relayed_by": raw_tx.get("relayed_by", ""),
            "raw_data": raw_tx  # Keep original for reference
        }
    
    def _print_transaction_details(self, tx: Dict) -> None:
        """
        Print detailed transaction information.
        
        Args:
            tx: Formatted transaction data
        """
        print(f"  📍 Hash: {tx['hash']}")
        print(f"  ⏰ Time: {tx['timestamp']}")
        print(f"  💰 Total Value: {tx['total_output_value']/100000000:.8f} BTC")
        print(f"  📊 Size: {tx['size']} bytes")
        print(f"  🔗 Inputs: {tx['input_count']} | Outputs: {tx['output_count']}")
        print(f"  🌐 Relayed by: {tx['relayed_by']}")
        print()
    
    def _print_summary(self) -> None:
        """Print summary of collected transaction data."""
        print("\n" + "=" * 60)
        print("📊 CONNECTION TEST SUMMARY")
        print("=" * 60)
        print(f"✅ Successfully collected: {len(self.collected_transactions)} transactions")
        
        if self.collected_transactions:
            total_value = sum(tx['total_output_value'] for tx in self.collected_transactions)
            avg_size = sum(tx['size'] for tx in self.collected_transactions) / len(self.collected_transactions)
            
            print(f"💰 Total BTC value observed: {total_value/100000000:.8f} BTC")
            print(f"📊 Average transaction size: {avg_size:.1f} bytes")
            print(f"🔗 Total unique input addresses: {len(set(addr for tx in self.collected_transactions for addr in tx['addresses']['inputs']))}")
            print(f"🔗 Total unique output addresses: {len(set(addr for tx in self.collected_transactions for addr in tx['addresses']['outputs']))}")
            
            print("\n📋 Sample Transaction Hashes:")
            for i, tx in enumerate(self.collected_transactions[:5], 1):
                print(f"  {i}. {tx['hash']}")
            
            if len(self.collected_transactions) > 5:
                print(f"  ... and {len(self.collected_transactions) - 5} more")
                
        print("\n🎯 Next Steps:")
        print("  • Connection to blockchain.com WebSocket API is working")
        print("  • Transaction data structure has been analyzed")
        print("  • Ready for Kafka integration in next phase")
        print("  • Data schema can be inferred from collected samples")
    
    def _print_troubleshooting_info(self) -> None:
        """Print troubleshooting information for failed connections."""
        print("\n" + "=" * 60)
        print("🔧 TROUBLESHOOTING INFORMATION")
        print("=" * 60)
        print("Common issues and solutions:")
        print("• Network connectivity: Check internet connection")
        print("• Firewall: Ensure WebSocket connections are allowed")
        print("• API limits: blockchain.com may have rate limits")
        print("• Service status: Check blockchain.com service status")
        print("\nFor more help, visit: https://www.blockchain.com/api")


def main():
    """Main function to run the Bitcoin transaction connection test."""
    try:
        # Create tester instance
        tester = BitcoinTransactionTester(max_transactions=10)
        
        # Run the connection test
        success = asyncio.run(tester.connect_and_test())
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()