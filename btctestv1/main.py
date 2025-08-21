"""
Bitcoin Blockchain Transaction WebSocket Connection Test

This script tests the connection to blockchain.com's WebSocket API to receive
real-time Bitcoin transaction data. This is a connection test only - no Kafka integration.
"""
import os
import json
import time
from datetime import datetime
from typing import Dict, Any, List
from websocket import WebSocketApp
import websocket

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BlockchainWebSocketTester:
    """
    Tests connection to blockchain.com WebSocket API for Bitcoin transaction data.
    Collects sample transactions for analysis and verification.
    """
    
    def __init__(self):
        """Initialize the WebSocket tester with configuration from environment variables."""
        self.websocket_url = os.environ.get("WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
        self.sample_count = int(os.environ.get("SAMPLE_COUNT", "10"))
        self.connection_timeout = int(os.environ.get("CONNECTION_TIMEOUT", "30"))
        
        self.collected_transactions = []
        self.ws = None
        self.connection_successful = False
        self.start_time = None
        
    def on_open(self, ws):
        """Called when WebSocket connection is opened."""
        print(f"✅ WebSocket connection opened successfully!")
        print(f"🔗 Connected to: {self.websocket_url}")
        print(f"📊 Requesting {self.sample_count} sample transactions...")
        print("=" * 60)
        
        self.connection_successful = True
        self.start_time = time.time()
        
        # Subscribe to unconfirmed transactions
        subscribe_message = {"op": "unconfirmed_sub"}
        ws.send(json.dumps(subscribe_message))
        print("📡 Subscribed to unconfirmed Bitcoin transactions")
        
    def on_message(self, ws, message):
        """Called when a message is received from WebSocket."""
        try:
            data = json.loads(message)
            
            # Check if this is a transaction message
            if data.get("op") == "utx" and "x" in data:
                transaction = data["x"]
                self.collected_transactions.append(transaction)
                
                tx_count = len(self.collected_transactions)
                print(f"\n📦 Transaction #{tx_count} received:")
                print(f"   Hash: {transaction.get('hash', 'N/A')}")
                print(f"   Size: {transaction.get('size', 'N/A')} bytes")
                print(f"   Inputs: {transaction.get('vin_sz', 'N/A')}")
                print(f"   Outputs: {transaction.get('vout_sz', 'N/A')}")
                print(f"   Time: {datetime.fromtimestamp(transaction.get('time', 0))}")
                
                # Print first input address for reference
                if transaction.get('inputs') and len(transaction['inputs']) > 0:
                    first_input = transaction['inputs'][0]
                    if 'prev_out' in first_input and 'addr' in first_input['prev_out']:
                        print(f"   From: {first_input['prev_out']['addr']}")
                
                # Print first output address for reference
                if transaction.get('out') and len(transaction['out']) > 0:
                    first_output = transaction['out'][0]
                    if 'addr' in first_output:
                        print(f"   To: {first_output['addr']}")
                        print(f"   Amount: {first_output.get('value', 0) / 100000000:.8f} BTC")
                
                # Stop after collecting the required number of samples
                if tx_count >= self.sample_count:
                    print(f"\n✅ Successfully collected {self.sample_count} sample transactions!")
                    ws.close()
                    
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse JSON message: {e}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def on_error(self, ws, error):
        """Called when WebSocket encounters an error."""
        print(f"❌ WebSocket error: {error}")
        
    def on_close(self, ws, close_status_code, close_msg):
        """Called when WebSocket connection is closed."""
        if self.connection_successful:
            elapsed_time = time.time() - self.start_time if self.start_time else 0
            print(f"\n🔌 WebSocket connection closed")
            print(f"⏱️  Session duration: {elapsed_time:.2f} seconds")
            print("=" * 60)
        else:
            print(f"❌ WebSocket connection closed unexpectedly")
            if close_msg:
                print(f"   Close message: {close_msg}")
                
    def print_summary_statistics(self):
        """Print summary statistics of collected transaction data."""
        if not self.collected_transactions:
            print("❌ No transactions collected!")
            return
            
        print(f"\n📊 SUMMARY STATISTICS")
        print("=" * 60)
        print(f"Total transactions collected: {len(self.collected_transactions)}")
        
        # Calculate basic statistics
        sizes = [tx.get('size', 0) for tx in self.collected_transactions]
        input_counts = [tx.get('vin_sz', 0) for tx in self.collected_transactions]
        output_counts = [tx.get('vout_sz', 0) for tx in self.collected_transactions]
        
        if sizes:
            print(f"Average transaction size: {sum(sizes) / len(sizes):.1f} bytes")
            print(f"Size range: {min(sizes)} - {max(sizes)} bytes")
            
        if input_counts:
            print(f"Average inputs per transaction: {sum(input_counts) / len(input_counts):.1f}")
            
        if output_counts:
            print(f"Average outputs per transaction: {sum(output_counts) / len(output_counts):.1f}")
            
        # Show data structure sample
        print(f"\n🔍 SAMPLE TRANSACTION STRUCTURE:")
        print("=" * 60)
        if self.collected_transactions:
            sample_tx = self.collected_transactions[0]
            # Pretty print the first transaction (truncated for readability)
            print(json.dumps(self._truncate_transaction_for_display(sample_tx), indent=2))
    
    def _truncate_transaction_for_display(self, tx: Dict[str, Any]) -> Dict[str, Any]:
        """Truncate transaction data for display purposes."""
        truncated = tx.copy()
        
        # Limit inputs and outputs to first 2 items for display
        if 'inputs' in truncated and len(truncated['inputs']) > 2:
            truncated['inputs'] = truncated['inputs'][:2] + [{"...": f"{len(tx['inputs']) - 2} more inputs"}]
            
        if 'out' in truncated and len(truncated['out']) > 2:
            truncated['out'] = truncated['out'][:2] + [{"...": f"{len(tx['out']) - 2} more outputs"}]
            
        return truncated
    
    def test_connection(self):
        """Main method to test the WebSocket connection."""
        print("🚀 Starting Bitcoin Blockchain WebSocket Connection Test")
        print("=" * 60)
        print(f"Target URL: {self.websocket_url}")
        print(f"Sample count: {self.sample_count}")
        print(f"Connection timeout: {self.connection_timeout}s")
        print("=" * 60)
        
        try:
            # Enable WebSocket debug logging if needed (uncomment if needed for debugging)
            # websocket.enableTrace(True)
            
            # Create WebSocket connection with callbacks
            self.ws = WebSocketApp(
                self.websocket_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            # Run the WebSocket connection
            self.ws.run_forever(ping_interval=30, ping_timeout=10)
            
        except Exception as e:
            print(f"❌ Failed to establish WebSocket connection: {e}")
            return False
            
        finally:
            # Print summary regardless of success/failure
            self.print_summary_statistics()
            
        return len(self.collected_transactions) > 0


def main():
    """Main function to run the connection test."""
    print("🔗 Bitcoin Blockchain WebSocket Connection Tester")
    print("📋 This is a CONNECTION TEST ONLY - no Kafka integration")
    print()
    
    # Verify required environment variables
    required_vars = ["WEBSOCKET_URL", "SAMPLE_COUNT"]
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("   Please set these variables in app.yaml or your environment")
        return
    
    # Create and run the tester
    tester = BlockchainWebSocketTester()
    
    try:
        success = tester.test_connection()
        
        if success:
            print("\n✅ CONNECTION TEST SUCCESSFUL!")
            print(f"   Successfully collected {len(tester.collected_transactions)} transactions")
            print("   The data structure above can be used for Kafka integration")
        else:
            print("\n❌ CONNECTION TEST FAILED!")
            print("   Please check your network connection and WebSocket URL")
            
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
        if tester.collected_transactions:
            print(f"   Collected {len(tester.collected_transactions)} transactions before interruption")
            tester.print_summary_statistics()
    
    except Exception as e:
        print(f"\n💥 Unexpected error during connection test: {e}")


if __name__ == "__main__":
    main()