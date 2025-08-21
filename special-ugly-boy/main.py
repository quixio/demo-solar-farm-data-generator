# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sources, see https://quix.io/docs/quix-streams/connectors/sources/index.html
from quixstreams import Application
from quixstreams.sources import Source

import os
import json
import time
from datetime import datetime
import threading

# Import websocket-client library
try:
    import websocket
    # Verify we have the correct websocket-client library
    if not hasattr(websocket, 'WebSocketApp'):
        raise ImportError("Wrong websocket module - need websocket-client package")
except ImportError as e:
    print(f"❌ Error importing websocket library: {e}")
    print("Please ensure 'websocket-client' package is installed")
    raise

def test_blockchain_websocket_connection():
    """
    A Quix Streams Source enables Applications to read data from something other
    than Kafka and publish it to a desired Kafka topic.

    You provide a Source to an Application, which will handle the Source's lifecycle.

    In this case, we have built a new Source that reads from a static set of
    already loaded json data representing a server's memory usage over time.

    There are numerous pre-built sources available to use out of the box; see:
    https://quix.io/docs/quix-streams/connectors/sources/index.html
    """

    memory_allocation_data = [
        {"m": "mem", "host": "host1", "used_percent": 64.56, "time": 1577836800000000000},
        {"m": "mem", "host": "host2", "used_percent": 71.89, "time": 1577836801000000000},
        {"m": "mem", "host": "host1", "used_percent": 63.27, "time": 1577836803000000000},
        {"m": "mem", "host": "host2", "used_percent": 73.45, "time": 1577836804000000000},
        {"m": "mem", "host": "host1", "used_percent": 62.98, "time": 1577836806000000000},
        {"m": "mem", "host": "host2", "used_percent": 74.33, "time": 1577836808000000000},
        {"m": "mem", "host": "host1", "used_percent": 65.21, "time": 1577836810000000000},
    ]

    def run(self):
        """
        Each Source must have a `run` method.

        It will include the logic behind your source, contained within a
        "while self.running" block for exiting when its parent Application stops.

        There a few methods on a Source available for producing to Kafka, like
        `self.serialize` and `self.produce`.
        """
        data = iter(self.memory_allocation_data)
        # either break when the app is stopped, or data is exhausted
        while self.running:
            try:
                event = next(data)
                event_serialized = self.serialize(key=event["host"], value=event)
                self.produce(key=event_serialized.key, value=event_serialized.value)
                print("Source produced event successfully!")
            except StopIteration:
                print("Source finished producing messages.")
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
        
        # Enable WebSocket debug traces for troubleshooting (if available)
        try:
            websocket.enableTrace(False)  # Set to True for detailed debugging
        except AttributeError as e:
            # enableTrace might not be available in some websocket implementations
            print(f"Note: WebSocket debug tracing not available - {e}")
            print("This is not critical for the connection test")
        
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
    
    # Verify websocket-client installation
    print("Checking websocket-client library...")
    try:
        print(f"✅ WebSocket library version: {websocket.__version__ if hasattr(websocket, '__version__') else 'unknown'}")
        print(f"✅ WebSocketApp available: {'Yes' if hasattr(websocket, 'WebSocketApp') else 'No'}")
        print(f"✅ enableTrace available: {'Yes' if hasattr(websocket, 'enableTrace') else 'No'}")
    except Exception as e:
        print(f"❌ Error checking websocket library: {e}")
    
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

#  Sources require execution under a conditional main
if __name__ == "__main__":
    main()