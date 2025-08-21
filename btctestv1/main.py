"""
Bitcoin Transaction Source for Quix Streams

This application connects to the blockchain.com WebSocket API to read live Bitcoin 
transaction data and publishes it to a Kafka topic using Quix Streams.

The application:
1. Connects to blockchain.com WebSocket API
2. Subscribes to unconfirmed Bitcoin transactions
3. Transforms raw transaction data into a structured format
4. Publishes messages to the configured Kafka output topic
5. Handles connection errors gracefully with retry logic
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from quixstreams import Application
from quixstreams.sources import Source

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class BitcoinWebSocketSource(Source):
    """
    A Quix Streams Source that reads Bitcoin transaction data from blockchain.com 
    WebSocket API and publishes it to Kafka.
    
    This source handles:
    - WebSocket connection to blockchain.com
    - Subscription management for unconfirmed transactions
    - Data transformation and structuring
    - Error handling and automatic reconnection
    - Production-ready message publishing to Kafka
    """
    
    def __init__(self, name: str, max_messages: int = 100):
        """
        Initialize the Bitcoin WebSocket source.
        
        Args:
            name: Name of the source
            max_messages: Maximum number of messages to process (for testing)
        """
        super().__init__(name)
        self.websocket_url = "wss://ws.blockchain.info/inv"
        self.max_messages = max_messages
        self.message_count = 0
        self.api_key = os.environ.get("API_KEY")  # Optional API key
        
    def run(self):
        """
        Main execution method for the source.
        Runs the WebSocket connection in an async event loop.
        """
        print("🚀 Starting Bitcoin Transaction Source")
        print(f"📡 Connecting to: {self.websocket_url}")
        print(f"🎯 Max messages to process: {self.max_messages}")
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Run the async WebSocket connection
        asyncio.run(self._run_websocket())
        
    async def _run_websocket(self):
        """
        Async method to handle WebSocket connection and message processing.
        """
        max_retries = 5
        retry_delay = 5
        
        while self.running and self.message_count < self.max_messages:
            for attempt in range(1, max_retries + 1):
                try:
                    print(f"\n🔄 Connection attempt {attempt}/{max_retries}...")
                    success = await self._attempt_connection()
                    
                    if success:
                        print("✅ Connection completed successfully!")
                        return
                        
                except Exception as e:
                    print(f"❌ Attempt {attempt} failed: {str(e)}")
                    
                    if attempt < max_retries and self.running:
                        print(f"⏳ Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, 60)  # Exponential backoff, max 60s
                    else:
                        print("🚫 All connection attempts failed!")
                        return
                        
            # Reset retry delay on successful connection
            retry_delay = 5
            
    async def _attempt_connection(self) -> bool:
        """
        Attempt to connect to blockchain.com WebSocket and process messages.
        
        Returns:
            bool: True if connection succeeded and processed messages
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
                
                # Process messages while running
                start_time = time.time()
                
                while (self.running and 
                       self.message_count < self.max_messages):
                    
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
                try:
                    unsubscribe_message = {"op": "unconfirmed_unsub"}
                    await websocket.send(json.dumps(unsubscribe_message))
                    print("📋 Unsubscribed from transactions")
                except:
                    pass  # Connection might be closed already
                
                return self.message_count > 0
                
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
        Process incoming WebSocket message and publish to Kafka.
        
        Args:
            message: Raw message from WebSocket
        """
        try:
            data = json.loads(message)
            
            # Debug: Print raw message structure for first few messages
            if self.message_count < 3:
                print(f"🔍 Raw message structure: {json.dumps(data, indent=2)[:500]}...")
            
            # Check if this is a transaction message
            if data.get("op") == "utx" and "x" in data:
                transaction = data["x"]
                
                # Transform transaction data to match schema
                formatted_tx = self._format_transaction(transaction)
                
                # Create Kafka message
                message_key = formatted_tx["hash"]
                message_value = formatted_tx
                
                # Serialize and produce to Kafka
                serialized_message = self.serialize(key=message_key, value=message_value)
                self.produce(key=serialized_message.key, value=serialized_message.value)
                
                self.message_count += 1
                print(f"📥 Transaction {self.message_count}/{self.max_messages} published: {formatted_tx['hash'][:16]}...")
                
                # Print detailed info for first few transactions
                if self.message_count <= 3:
                    self._print_transaction_details(formatted_tx)
                    
        except json.JSONDecodeError:
            print("⚠️ Received invalid JSON message, skipping...")
        except Exception as e:
            print(f"⚠️ Error processing message: {str(e)}")
    
    def _format_transaction(self, raw_tx: Dict) -> Dict:
        """
        Format raw transaction data into the expected schema format.
        
        Args:
            raw_tx: Raw transaction data from blockchain.com
            
        Returns:
            Dict: Formatted transaction data matching the schema
        """
        # Calculate total values from inputs and outputs
        total_input_value = sum(
            inp.get("prev_out", {}).get("value", 0) 
            for inp in raw_tx.get("inputs", [])
        )
        
        total_output_value = sum(
            out.get("value", 0) 
            for out in raw_tx.get("out", [])
        )
        
        # Format according to schema
        return {
            "hash": raw_tx.get("hash", ""),
            "time": datetime.fromtimestamp(raw_tx.get("time", 0)).isoformat() if raw_tx.get("time") else datetime.now().isoformat(),
            "total_value": float(total_output_value / 100000000),  # Convert satoshis to BTC
            "size": int(raw_tx.get("size", 0)),
            "inputs": int(raw_tx.get("vin_sz", 0)),
            "outputs": int(raw_tx.get("vout_sz", 0)),
            "relayed_by": raw_tx.get("relayed_by", ""),
            # Additional useful fields
            "fee": float(raw_tx.get("fee", 0) / 100000000) if raw_tx.get("fee") else 0.0,
            "timestamp_unix": raw_tx.get("time", 0),
            "input_addresses": [
                inp.get("prev_out", {}).get("addr")
                for inp in raw_tx.get("inputs", [])
                if inp.get("prev_out", {}).get("addr")
            ],
            "output_addresses": [
                out.get("addr")
                for out in raw_tx.get("out", [])
                if out.get("addr")
            ],
            "total_input_value": float(total_input_value / 100000000),
            "total_output_value": float(total_output_value / 100000000)
        }
    
    def _print_transaction_details(self, tx: Dict) -> None:
        """
        Print detailed transaction information for debugging.
        
        Args:
            tx: Formatted transaction data
        """
        print(f"  📍 Hash: {tx['hash']}")
        print(f"  ⏰ Time: {tx['time']}")
        print(f"  💰 Total Value: {tx['total_value']:.8f} BTC")
        print(f"  📊 Size: {tx['size']} bytes")
        print(f"  🔗 Inputs: {tx['inputs']} | Outputs: {tx['outputs']}")
        print(f"  🌐 Relayed by: {tx['relayed_by']}")
        if tx['fee'] > 0:
            print(f"  💸 Fee: {tx['fee']:.8f} BTC")
        print()


def main():
    """
    Main function to set up and run the Bitcoin transaction source application.
    """
    print("=" * 60)
    print("🔗 Bitcoin Transaction Source - Quix Streams")
    print("=" * 60)
    
    try:
        # Get output topic from environment variables
        output_topic_name = os.environ.get("output", "")
        if not output_topic_name:
            raise ValueError("output environment variable is required")
        
        print(f"📤 Output topic: {output_topic_name}")
        
        # Create Quix Streams application
        app = Application(
            consumer_group="bitcoin_transaction_source",
            auto_create_topics=True
        )
        
        # Create output topic
        output_topic = app.topic(name=output_topic_name)
        
        # Create Bitcoin WebSocket source with limit for testing
        bitcoin_source = BitcoinWebSocketSource(
            name="bitcoin-websocket-source",
            max_messages=100  # Limit for testing
        )
        
        # Add source to application - this will handle the lifecycle
        app.add_source(source=bitcoin_source, topic=output_topic)
        
        print("🏃 Starting application...")
        
        # Run the application
        app.run()
        
        print("\n✅ Application completed successfully!")
        
    except KeyboardInterrupt:
        print("\n🛑 Application interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Application error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()