#!/usr/bin/env python3
"""
Blockchain.com WebSocket Connection Test

This script tests the connection to the Blockchain.com WebSocket API
to read Bitcoin transaction data. This is a connection test only - no
Kafka/Quix integration yet.

Supported subscription types:
- unconfirmed_sub: All new Bitcoin transactions
- addr_sub: Transactions for a specific Bitcoin address
"""

import os
import json
import time
import logging
from typing import Optional, Dict, Any
import websocket
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BlockchainWebSocketTest:
    """Test connection to Blockchain.com WebSocket API"""
    
    def __init__(self):
        self.websocket_url = os.environ.get("WEBSOCKET_URL", "wss://ws.blockchain.info/inv")
        self.subscription_type = os.environ.get("SUBSCRIPTION_TYPE", "unconfirmed_sub")
        self.bitcoin_address = os.environ.get("BITCOIN_ADDRESS", "")
        
        self.ws: Optional[websocket.WebSocketApp] = None
        self.received_messages = []
        self.target_message_count = 10
        self.connection_established = False
        self.error_occurred = False
        self.error_message = ""
        
    def on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        """Handle incoming WebSocket messages"""
        try:
            # Parse the JSON message
            data = json.loads(message)
            
            # Add to our collection
            self.received_messages.append(data)
            
            # Print formatted message
            print(f"\n--- Message {len(self.received_messages)} ---")
            print(f"Operation: {data.get('op', 'unknown')}")
            
            if data.get('op') == 'utx':
                # Unconfirmed transaction
                tx_data = data.get('x', {})
                print(f"Transaction Hash: {tx_data.get('hash', 'unknown')}")
                print(f"Transaction Size: {tx_data.get('size', 'unknown')} bytes")
                print(f"Input Count: {tx_data.get('vin_sz', 'unknown')}")
                print(f"Output Count: {tx_data.get('vout_sz', 'unknown')}")
                print(f"Time: {tx_data.get('time', 'unknown')}")
                
                # Show input/output summary
                inputs = tx_data.get('inputs', [])
                outputs = tx_data.get('out', [])
                
                total_input_value = 0
                for inp in inputs:
                    prev_out = inp.get('prev_out', {})
                    total_input_value += prev_out.get('value', 0)
                
                total_output_value = sum(out.get('value', 0) for out in outputs)
                
                print(f"Total Input Value: {total_input_value / 100000000:.8f} BTC")
                print(f"Total Output Value: {total_output_value / 100000000:.8f} BTC")
                print(f"Transaction Fee: {(total_input_value - total_output_value) / 100000000:.8f} BTC")
                
            elif data.get('op') == 'block':
                # New block
                block_data = data.get('x', {})
                print(f"Block Hash: {block_data.get('hash', 'unknown')}")
                print(f"Block Height: {block_data.get('height', 'unknown')}")
                print(f"Transaction Count: {block_data.get('nTx', 'unknown')}")
                print(f"Block Size: {block_data.get('size', 'unknown')} bytes")
                print(f"Time: {block_data.get('time', 'unknown')}")
                
            else:
                # Other message types
                print(f"Raw message: {json.dumps(data, indent=2)}")
                
            print("=" * 50)
            
            # Stop after receiving target number of messages
            if len(self.received_messages) >= self.target_message_count:
                logger.info(f"Received {self.target_message_count} messages. Closing connection.")
                ws.close()
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON message: {e}")
            logger.error(f"Raw message: {message}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        """Handle WebSocket errors"""
        self.error_occurred = True
        self.error_message = str(error)
        logger.error(f"WebSocket error: {error}")
    
    def on_close(self, ws: websocket.WebSocketApp, close_status_code: int, close_msg: str) -> None:
        """Handle WebSocket close"""
        logger.info("WebSocket connection closed")
        if close_status_code:
            logger.info(f"Close status code: {close_status_code}")
        if close_msg:
            logger.info(f"Close message: {close_msg}")
    
    def on_open(self, ws: websocket.WebSocketApp) -> None:
        """Handle WebSocket open"""
        logger.info("WebSocket connection established")
        self.connection_established = True
        
        try:
            # Send subscription message
            if self.subscription_type == "addr_sub":
                if not self.bitcoin_address:
                    raise ValueError("BITCOIN_ADDRESS is required when using addr_sub subscription type")
                
                subscribe_msg = {
                    "op": "addr_sub",
                    "addr": self.bitcoin_address
                }
                logger.info(f"Subscribing to address: {self.bitcoin_address}")
                
            else:  # Default to unconfirmed_sub
                subscribe_msg = {
                    "op": "unconfirmed_sub"
                }
                logger.info("Subscribing to all unconfirmed transactions")
            
            ws.send(json.dumps(subscribe_msg))
            logger.info(f"Subscription message sent: {subscribe_msg}")
            
        except Exception as e:
            logger.error(f"Failed to send subscription message: {e}")
            self.error_occurred = True
            self.error_message = str(e)
            ws.close()
    
    def run_test(self) -> bool:
        """Run the connection test"""
        print("=" * 60)
        print("BLOCKCHAIN.COM WEBSOCKET CONNECTION TEST")
        print("=" * 60)
        print(f"WebSocket URL: {self.websocket_url}")
        print(f"Subscription Type: {self.subscription_type}")
        if self.bitcoin_address:
            print(f"Bitcoin Address: {self.bitcoin_address}")
        print(f"Target Messages: {self.target_message_count}")
        print("=" * 60)
        
        try:
            # Enable WebSocket debug logging for troubleshooting
            websocket.enableTrace(False)  # Set to True for detailed debugging
            
            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                self.websocket_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )
            
            # Start the WebSocket in a separate thread with timeout
            logger.info("Connecting to Blockchain.com WebSocket...")
            
            # Use run_forever with a ping interval to keep connection alive
            self.ws.run_forever(
                ping_interval=30,
                ping_timeout=10
            )
            
            # Check results
            if self.error_occurred:
                logger.error(f"Connection test failed: {self.error_message}")
                return False
            
            if not self.connection_established:
                logger.error("Failed to establish WebSocket connection")
                return False
                
            if len(self.received_messages) == 0:
                logger.warning("No messages received - this might indicate connection issues or low activity")
                return False
            
            # Summary
            print("\n" + "=" * 60)
            print("CONNECTION TEST SUMMARY")
            print("=" * 60)
            print(f"✓ Connection Status: SUCCESS")
            print(f"✓ Messages Received: {len(self.received_messages)}")
            print(f"✓ Subscription Type: {self.subscription_type}")
            
            # Show message type distribution
            message_types = {}
            for msg in self.received_messages:
                msg_type = msg.get('op', 'unknown')
                message_types[msg_type] = message_types.get(msg_type, 0) + 1
            
            print(f"✓ Message Types: {dict(message_types)}")
            print("=" * 60)
            
            logger.info("Connection test completed successfully!")
            return True
            
        except KeyboardInterrupt:
            logger.info("Test interrupted by user")
            if self.ws:
                self.ws.close()
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error during connection test: {e}")
            return False


def main():
    """Main function to run the connection test"""
    
    # Validate environment variables
    required_vars = ["WEBSOCKET_URL", "SUBSCRIPTION_TYPE"]
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        print("\nRequired environment variables:")
        print("- WEBSOCKET_URL: Blockchain.com WebSocket URL")
        print("- SUBSCRIPTION_TYPE: unconfirmed_sub or addr_sub")
        print("- BITCOIN_ADDRESS: Required only if SUBSCRIPTION_TYPE is addr_sub")
        return
    
    # Additional validation for address subscription
    subscription_type = os.environ.get("SUBSCRIPTION_TYPE")
    if subscription_type == "addr_sub" and not os.environ.get("BITCOIN_ADDRESS"):
        logger.error("BITCOIN_ADDRESS is required when SUBSCRIPTION_TYPE is 'addr_sub'")
        return
    
    # Run the test
    test = BlockchainWebSocketTest()
    success = test.run_test()
    
    if success:
        print("\n🎉 Connection test completed successfully!")
        print("The application is ready to connect to Blockchain.com WebSocket API.")
    else:
        print("\n❌ Connection test failed!")
        print("Please check the error messages above for troubleshooting.")


if __name__ == "__main__":
    main()