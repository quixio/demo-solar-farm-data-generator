#!/usr/bin/env python3
"""
Bitcoin Transaction Connection Test

This script tests the connection to blockchain.com's WebSocket API to receive
real-time Bitcoin transaction data. This is a connection test only - no Kafka
integration is included yet.

The script will:
1. Connect to blockchain.com's websocket
2. Subscribe to unconfirmed transactions
3. Read exactly 10 sample transactions
4. Display formatted transaction data
5. Close the connection gracefully
"""

import os
import json
import time
import threading
from typing import List, Dict, Any, Optional
import websocket
from datetime import datetime

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BlockchainWebSocketTester:
    """
    A connection tester for blockchain.com's WebSocket API.
    
    This class handles the WebSocket connection, subscription management,
    and data collection for testing purposes.
    """
    
    def __init__(self, websocket_url: str, connection_timeout: int = 30, sample_count: int = 10):
        """
        Initialize the WebSocket tester.
        
        Args:
            websocket_url: The WebSocket URL to connect to
            connection_timeout: Connection timeout in seconds
            sample_count: Number of sample transactions to collect
        """
        self.websocket_url = websocket_url
        self.connection_timeout = connection_timeout
        self.sample_count = sample_count
        self.collected_transactions: List[Dict[str, Any]] = []
        self.ws: Optional[websocket.WebSocketApp] = None
        self.connected = False
        self.error_message = None
        self.lock = threading.Lock()
        
    def on_open(self, ws):
        """Handle WebSocket connection opening."""
        print(f"✅ Connected to blockchain.com WebSocket at {datetime.now()}")
        self.connected = True
        
        # Subscribe to unconfirmed transactions
        subscription_message = {"op": "unconfirmed_sub"}
        try:
            ws.send(json.dumps(subscription_message))
            print("📡 Subscribed to unconfirmed Bitcoin transactions")
        except Exception as e:
            self.error_message = f"Failed to send subscription message: {str(e)}"
            print(f"❌ {self.error_message}")
    
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            data = json.loads(message)
            
            # Check if this is a transaction message
            if data.get("op") == "utx" and "x" in data:
                with self.lock:
                    if len(self.collected_transactions) < self.sample_count:
                        transaction = data["x"]
                        self.collected_transactions.append(transaction)
                        
                        print(f"📨 Received transaction {len(self.collected_transactions)}/{self.sample_count}")
                        self._print_transaction_summary(transaction, len(self.collected_transactions))
                        
                        # Close connection once we have enough samples
                        if len(self.collected_transactions) >= self.sample_count:
                            print(f"\n✅ Collected {self.sample_count} sample transactions. Closing connection...")
                            ws.close()
                            
        except json.JSONDecodeError as e:
            print(f"⚠️  Received non-JSON message: {message[:100]}...")
        except Exception as e:
            self.error_message = f"Error processing message: {str(e)}"
            print(f"❌ {self.error_message}")
    
    def on_error(self, ws, error):
        """Handle WebSocket errors."""
        self.error_message = f"WebSocket error: {str(error)}"
        print(f"❌ {self.error_message}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection closing."""
        print(f"🔌 WebSocket connection closed at {datetime.now()}")
        if close_status_code:
            print(f"   Close code: {close_status_code}, Message: {close_msg}")
    
    def _print_transaction_summary(self, transaction: Dict[str, Any], count: int):
        """Print a formatted summary of the transaction."""
        print(f"\n--- Transaction #{count} ---")
        print(f"Hash: {transaction.get('hash', 'N/A')}")
        print(f"Time: {datetime.fromtimestamp(transaction.get('time', 0))}")
        print(f"Size: {transaction.get('size', 0)} bytes")
        print(f"Inputs: {transaction.get('vin_sz', 0)}")
        print(f"Outputs: {transaction.get('vout_sz', 0)}")
        
        # Calculate total input/output values
        total_input = sum(inp.get('prev_out', {}).get('value', 0) for inp in transaction.get('inputs', []))
        total_output = sum(out.get('value', 0) for out in transaction.get('out', []))
        
        print(f"Total Input: {total_input / 100000000:.8f} BTC")
        print(f"Total Output: {total_output / 100000000:.8f} BTC")
        print(f"Fee: {(total_input - total_output) / 100000000:.8f} BTC")
        print(f"Relayed by: {transaction.get('relayed_by', 'N/A')}")
        print("-" * 40)
    
    def test_connection(self) -> bool:
        """
        Test the WebSocket connection and collect sample data.
        
        Returns:
            True if successful, False otherwise
        """
        print(f"🚀 Starting Bitcoin transaction connection test...")
        print(f"WebSocket URL: {self.websocket_url}")
        print(f"Sample count: {self.sample_count}")
        print(f"Connection timeout: {self.connection_timeout}s")
        print("-" * 60)
        
        try:
            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                self.websocket_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            # Start the WebSocket in a separate thread
            ws_thread = threading.Thread(target=self.ws.run_forever)
            ws_thread.daemon = True
            ws_thread.start()
            
            # Wait for connection or timeout
            start_time = time.time()
            while not self.connected and not self.error_message and (time.time() - start_time) < self.connection_timeout:
                time.sleep(0.1)
            
            if not self.connected:
                print(f"❌ Failed to connect within {self.connection_timeout} seconds")
                if self.error_message:
                    print(f"❌ Error: {self.error_message}")
                return False
            
            # Wait for sample collection or timeout (extended timeout for data collection)
            collection_timeout = max(120, self.connection_timeout * 2)  # At least 2 minutes
            start_collection = time.time()
            
            while (len(self.collected_transactions) < self.sample_count and 
                   not self.error_message and 
                   (time.time() - start_collection) < collection_timeout):
                time.sleep(0.1)
            
            # Close connection if still open
            if self.ws:
                self.ws.close()
            
            # Wait for the thread to finish
            ws_thread.join(timeout=5)
            
            if self.error_message:
                print(f"❌ Error during data collection: {self.error_message}")
                return False
            
            if len(self.collected_transactions) == 0:
                print("❌ No transactions received - this might indicate a connection issue")
                return False
            
            print(f"\n🎉 Connection test completed successfully!")
            print(f"📊 Collected {len(self.collected_transactions)} transactions out of {self.sample_count} requested")
            
            return True
            
        except Exception as e:
            print(f"❌ Unexpected error during connection test: {str(e)}")
            return False
    
    def print_detailed_summary(self):
        """Print detailed summary of collected data."""
        if not self.collected_transactions:
            print("📭 No transaction data available for summary.")
            return
        
        print(f"\n{'='*60}")
        print(f"DETAILED TRANSACTION DATA SUMMARY")
        print(f"{'='*60}")
        print(f"Total transactions collected: {len(self.collected_transactions)}")
        
        # Analyze transaction patterns
        sizes = [tx.get('size', 0) for tx in self.collected_transactions]
        input_counts = [tx.get('vin_sz', 0) for tx in self.collected_transactions]
        output_counts = [tx.get('vout_sz', 0) for tx in self.collected_transactions]
        
        print(f"\nTransaction Size Statistics:")
        print(f"  Average size: {sum(sizes) / len(sizes):.2f} bytes")
        print(f"  Min size: {min(sizes)} bytes")
        print(f"  Max size: {max(sizes)} bytes")
        
        print(f"\nInput/Output Statistics:")
        print(f"  Average inputs per transaction: {sum(input_counts) / len(input_counts):.2f}")
        print(f"  Average outputs per transaction: {sum(output_counts) / len(output_counts):.2f}")
        
        print(f"\nSample Transaction Hashes:")
        for i, tx in enumerate(self.collected_transactions[:5], 1):
            print(f"  {i}. {tx.get('hash', 'N/A')}")
        
        if len(self.collected_transactions) > 5:
            print(f"  ... and {len(self.collected_transactions) - 5} more")
        
        print(f"\n📋 Data Structure Analysis:")
        if self.collected_transactions:
            sample_tx = self.collected_transactions[0]
            print(f"Available fields in transaction data:")
            for key in sorted(sample_tx.keys()):
                value = sample_tx[key]
                if isinstance(value, list):
                    print(f"  - {key}: array with {len(value)} items")
                elif isinstance(value, dict):
                    print(f"  - {key}: object with {len(value)} fields")
                else:
                    print(f"  - {key}: {type(value).__name__}")
        
        print(f"{'='*60}")


def main():
    """Main function to run the connection test."""
    print("Bitcoin Transaction WebSocket Connection Test")
    print("=" * 60)
    
    # Get configuration from environment variables
    websocket_url = os.environ.get("WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
    connection_timeout = int(os.environ.get("CONNECTION_TIMEOUT", "30"))
    sample_count = int(os.environ.get("SAMPLE_COUNT", "10"))
    
    # Validate configuration
    if not websocket_url:
        print("❌ Error: WEBSOCKET_URL environment variable is required")
        return False
    
    if sample_count <= 0:
        print("❌ Error: SAMPLE_COUNT must be greater than 0")
        return False
    
    if connection_timeout <= 0:
        print("❌ Error: CONNECTION_TIMEOUT must be greater than 0")
        return False
    
    # Create and run the tester
    tester = BlockchainWebSocketTester(
        websocket_url=websocket_url,
        connection_timeout=connection_timeout,
        sample_count=sample_count
    )
    
    success = tester.test_connection()
    
    if success:
        tester.print_detailed_summary()
        print("\n✅ Connection test completed successfully!")
        print("🔧 Next steps: This connection can now be integrated with Quix Streams")
    else:
        print("\n❌ Connection test failed!")
        print("🔧 Check your network connection and try again")
    
    return success


if __name__ == "__main__":
    main()