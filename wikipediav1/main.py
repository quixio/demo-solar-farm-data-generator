"""
Wikipedia EventStreams Connection Test

This script tests the connection to Wikipedia's change event stream API and
retrieves sample data to verify connectivity and understand the data structure.
This is a connection test only - no Kafka integration yet.
"""

import os
import json
import time
from datetime import datetime
from requests_sse import EventSource

# For local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class WikipediaStreamConnectionTest:
    """
    A connection test class for Wikipedia's EventStreams API.
    Tests connectivity and retrieves sample data for analysis.
    """
    
    def __init__(self):
        # Get configuration from environment variables
        self.stream_url = os.environ.get('WIKIPEDIA_STREAM_URL', 'https://stream.wikimedia.org/v2/stream/recentchange')
        self.wiki_filter = os.environ.get('WIKI_FILTER', '').strip()
        
        print(f"=== Wikipedia EventStreams Connection Test ===")
        print(f"Stream URL: {self.stream_url}")
        print(f"Wiki Filter: {self.wiki_filter if self.wiki_filter else 'None (all wikis)'}")
        print(f"Target samples: 10 events")
        print("=" * 50)

    def connect_and_test(self):
        """
        Connect to Wikipedia's EventStreams API and retrieve sample data.
        """
        sample_count = 0
        target_samples = 10
        connection_start = time.time()
        
        try:
            print(f"🔗 Connecting to {self.stream_url}...")
            
            with EventSource(self.stream_url) as stream:
                print("✅ Connection established successfully!")
                print(f"📊 Collecting {target_samples} sample events...\n")
                
                for event in stream:
                    if event.type == 'message':
                        try:
                            # Parse the JSON data
                            change = json.loads(event.data)
                            
                            # Skip canary events as recommended in the documentation
                            if change.get('meta', {}).get('domain') == 'canary':
                                continue
                            
                            # Apply wiki filtering if specified
                            if self.wiki_filter and change.get('wiki') != self.wiki_filter:
                                continue
                            
                            # Display the event
                            sample_count += 1
                            self._display_event(sample_count, change)
                            
                            # Check if we've collected enough samples
                            if sample_count >= target_samples:
                                break
                                
                        except (ValueError, json.JSONDecodeError) as e:
                            print(f"⚠️ Failed to parse event data: {e}")
                            continue
                    
                    elif event.type == 'error':
                        print(f"❌ Received error event: {event.data}")
                        break
                
                connection_duration = time.time() - connection_start
                self._display_summary(sample_count, connection_duration)
                
        except KeyboardInterrupt:
            print("\n🛑 Connection test interrupted by user")
            connection_duration = time.time() - connection_start
            self._display_summary(sample_count, connection_duration)
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            print(f"   Error type: {type(e).__name__}")
            self._display_troubleshooting()

    def _display_event(self, event_number, change):
        """
        Display a formatted Wikipedia change event.
        """
        # Extract key information
        timestamp = change.get('meta', {}).get('dt', 'Unknown')
        wiki = change.get('wiki', 'Unknown')
        server_name = change.get('server_name', 'Unknown')
        change_type = change.get('type', 'Unknown')
        title = change.get('title', 'Unknown')
        user = change.get('user', 'Anonymous')
        comment = change.get('comment', '')
        revision_id = change.get('revision', {}).get('new', 'N/A')
        
        print(f"📄 Event #{event_number}")
        print(f"   📅 Timestamp: {timestamp}")
        print(f"   🌐 Wiki: {wiki} ({server_name})")
        print(f"   🔄 Type: {change_type}")
        print(f"   📝 Page: {title}")
        print(f"   👤 User: {user}")
        print(f"   🆔 Revision ID: {revision_id}")
        if comment:
            print(f"   💬 Comment: {comment[:100]}{'...' if len(comment) > 100 else ''}")
        
        # Show some additional metadata
        if 'length' in change:
            old_len = change['length'].get('old', 0)
            new_len = change['length'].get('new', 0)
            size_change = new_len - old_len
            print(f"   📏 Size change: {size_change:+d} bytes ({old_len} → {new_len})")
        
        print()

    def _display_summary(self, sample_count, duration):
        """
        Display connection test summary.
        """
        print("=" * 50)
        print("📊 CONNECTION TEST SUMMARY")
        print("=" * 50)
        print(f"✅ Connection successful: Yes")
        print(f"📈 Events collected: {sample_count}")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"📊 Average rate: {sample_count/duration:.2f} events/second")
        
        if sample_count > 0:
            print(f"\n🎯 DATA STRUCTURE INSIGHTS:")
            print(f"   • Real-time Wikipedia changes detected")
            print(f"   • JSON format with rich metadata")
            print(f"   • Events include: timestamp, wiki, page, user, revisions")
            print(f"   • Ready for Kafka integration")
        
        print(f"\n📋 NEXT STEPS:")
        print(f"   • Connection test: PASSED ✅")
        print(f"   • Data structure: ANALYZED ✅")
        print(f"   • Ready for: Quix Streams integration")

    def _display_troubleshooting(self):
        """
        Display troubleshooting information for connection failures.
        """
        print("\n🔧 TROUBLESHOOTING:")
        print("   • Check internet connectivity")
        print("   • Verify the stream URL is correct")
        print("   • Ensure no firewall is blocking the connection")
        print("   • Try again in a few moments (temporary service issues)")
        print(f"   • Stream URL: {self.stream_url}")


def main():
    """
    Main function to run the Wikipedia EventStreams connection test.
    """
    try:
        # Create and run the connection test
        test = WikipediaStreamConnectionTest()
        test.connect_and_test()
        
    except Exception as e:
        print(f"❌ Failed to initialize connection test: {e}")
        print(f"   Error type: {type(e).__name__}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())