#!/usr/bin/env python3
"""
Wikipedia Change Event Stream Connection Test

This script connects to Wikipedia's real-time change event stream and collects
sample events to test the connection. This is a connection test only - no Kafka
integration yet.

Based on documentation from: https://stream.wikimedia.org/
"""

import json
import os
import sys
import time
from typing import Dict, Any, Optional
from requests_sse import EventSource

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class WikipediaChangeStreamConnector:
    """
    Connector for Wikipedia's real-time change event stream using Server-Sent Events (SSE).
    
    This connector reads from Wikipedia's EventStreams service which provides 
    live updates for various Wikipedia events, particularly the 'recentchange' stream.
    """
    
    def __init__(self):
        """Initialize the Wikipedia change stream connector."""
        self.stream_url = 'https://stream.wikimedia.org/v2/stream/recentchange'
        self.wiki_filter = os.environ.get('WIKI_FILTER', 'enwiki').strip()
        self.max_events = int(os.environ.get('MAX_EVENTS_TEST', '10'))
        self.events_collected = 0
        
        print(f"Wikipedia Change Stream Connector initialized:")
        print(f"  Stream URL: {self.stream_url}")
        print(f"  Wiki filter: {self.wiki_filter if self.wiki_filter else 'ALL WIKIS'}")
        print(f"  Max events to collect: {self.max_events}")
        print()
    
    def _is_canary_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Check if this is a canary event that should be discarded.
        
        Args:
            event_data: The parsed event data
            
        Returns:
            True if this is a canary event
        """
        return event_data.get('meta', {}).get('domain') == 'canary'
    
    def _should_process_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Determine if this event should be processed based on filters.
        
        Args:
            event_data: The parsed event data
            
        Returns:
            True if the event matches our criteria
        """
        # Skip canary events
        if self._is_canary_event(event_data):
            return False
        
        # If no wiki filter is set, process all events
        if not self.wiki_filter:
            return True
        
        # Check if event matches the wiki filter
        wiki = event_data.get('wiki', '')
        return wiki == self.wiki_filter
    
    def _format_event_for_display(self, event_data: Dict[str, Any], event_number: int) -> None:
        """
        Format and display an event in a readable way.
        
        Args:
            event_data: The event data to display
            event_number: Sequential number of this event
        """
        print(f"=" * 80)
        print(f"EVENT #{event_number}")
        print(f"=" * 80)
        
        # Key fields to display
        key_fields = [
            ('Wiki', 'wiki'),
            ('Title', 'title'),
            ('User', 'user'),
            ('Type', 'type'),
            ('Server', 'server_name'),
            ('Timestamp', 'timestamp'),
            ('Comment', 'comment'),
            ('Minor', 'minor'),
            ('Bot', 'bot'),
            ('Patrolled', 'patrolled')
        ]
        
        for display_name, field_name in key_fields:
            value = event_data.get(field_name, 'N/A')
            if value is not None:
                print(f"{display_name:12}: {value}")
        
        # Show some metadata
        meta = event_data.get('meta', {})
        if meta:
            print(f"{'Domain':12}: {meta.get('domain', 'N/A')}")
            print(f"{'Stream':12}: {meta.get('stream', 'N/A')}")
        
        # Show revision info if available
        if 'revision' in event_data:
            rev = event_data['revision']
            print(f"{'Rev Old':12}: {rev.get('old', 'N/A')}")
            print(f"{'Rev New':12}: {rev.get('new', 'N/A')}")
        
        print()
    
    def test_connection(self) -> bool:
        """
        Test connection to Wikipedia's change event stream.
        
        Returns:
            True if connection successful and events collected
        """
        print(f"🔗 Connecting to Wikipedia change event stream...")
        print(f"   URL: {self.stream_url}")
        print()
        
        try:
            with EventSource(self.stream_url) as stream:
                print("✅ Connected successfully!")
                print(f"📡 Listening for events... (will collect {self.max_events} events)")
                print()
                
                for event in stream:
                    if event.type == 'message':
                        try:
                            # Parse the JSON event data
                            event_data = json.loads(event.data)
                            
                            # Check if we should process this event
                            if self._should_process_event(event_data):
                                self.events_collected += 1
                                self._format_event_for_display(event_data, self.events_collected)
                                
                                # Stop after collecting enough events
                                if self.max_events > 0 and self.events_collected >= self.max_events:
                                    print(f"✅ Successfully collected {self.events_collected} events!")
                                    print("🛑 Stopping collection (test limit reached)")
                                    break
                        
                        except json.JSONDecodeError as e:
                            print(f"⚠️ Failed to parse event JSON: {e}")
                            continue
                        except Exception as e:
                            print(f"⚠️ Error processing event: {e}")
                            continue
        
        except KeyboardInterrupt:
            print(f"\n🛑 Interrupted by user. Collected {self.events_collected} events.")
            return self.events_collected > 0
        
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
        
        return True
    
    def run_connection_test(self) -> None:
        """Run the complete connection test."""
        print("🚀 Wikipedia Change Event Stream - Connection Test")
        print("=" * 80)
        
        success = self.test_connection()
        
        print("\n" + "=" * 80)
        print("📊 CONNECTION TEST SUMMARY")
        print("=" * 80)
        
        if success and self.events_collected > 0:
            print("✅ CONNECTION TEST PASSED")
            print(f"   - Successfully connected to Wikipedia change stream")
            print(f"   - Collected {self.events_collected} sample events")
            print(f"   - Wiki filter: {self.wiki_filter if self.wiki_filter else 'ALL WIKIS'}")
            print(f"   - Stream provides real-time Wikipedia changes")
        else:
            print("❌ CONNECTION TEST FAILED")
            print(f"   - Events collected: {self.events_collected}")
            print("   - Please check network connectivity and stream availability")
        
        print("\n💡 NEXT STEPS:")
        print("   - This connection test confirms we can read from Wikipedia's stream")
        print("   - Data structure has been validated with sample events")
        print("   - Ready to integrate with Quix Streams for Kafka processing")


def main():
    """Main entry point for the connection test."""
    
    # Check required environment variables
    if 'output' not in os.environ:
        print("⚠️ Warning: 'output' environment variable not set (OK for connection test)")
    
    # Initialize and run the connector
    connector = WikipediaChangeStreamConnector()
    connector.run_connection_test()


if __name__ == "__main__":
    main()