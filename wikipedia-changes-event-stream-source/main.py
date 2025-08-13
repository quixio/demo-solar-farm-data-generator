# DEPENDENCIES:
# pip install requests-sse
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import sys
from datetime import datetime
from dotenv import load_dotenv
from requests_sse import EventSource

# Load environment variables
load_dotenv()

def connect_and_read_wikipedia_changes():
    """
    Connect to Wikipedia changes event stream and read 10 sample page edit events.
    """
    
    # Wikipedia EventStreams URL for recent changes
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    
    print("=" * 80)
    print("Connecting to Wikipedia Changes Event Stream")
    print(f"URL: {url}")
    print("=" * 80)
    
    events_read = 0
    max_events = 10
    
    try:
        with EventSource(url, timeout=30) as stream:
            print("\n✓ Successfully connected to stream\n")
            print("Reading 10 sample page edit events...\n")
            
            for event in stream:
                if event.type == 'message':
                    try:
                        # Parse the JSON data
                        change = json.loads(event.data)
                        
                        # Skip canary events (test events)
                        if change.get('meta', {}).get('domain') == 'canary':
                            continue
                        
                        # Filter for edit events only (as requested for page edits)
                        if change.get('type') != 'edit':
                            continue
                        
                        events_read += 1
                        
                        # Extract basic metadata about the page edit
                        print(f"--- Event {events_read}/{max_events} ---")
                        print(f"Timestamp: {change.get('meta', {}).get('dt', 'N/A')}")
                        print(f"Wiki: {change.get('wiki', 'N/A')}")
                        print(f"Page Title: {change.get('title', 'N/A')}")
                        print(f"User: {change.get('user', 'N/A')}")
                        print(f"Edit Type: {change.get('type', 'N/A')}")
                        print(f"Namespace: {change.get('namespace', 'N/A')}")
                        print(f"Comment: {change.get('comment', 'N/A')[:100]}...")  # Truncate long comments
                        print(f"Old Revision ID: {change.get('revision', {}).get('old', 'N/A')}")
                        print(f"New Revision ID: {change.get('revision', {}).get('new', 'N/A')}")
                        print(f"Bot Edit: {change.get('bot', False)}")
                        print(f"Minor Edit: {change.get('minor', False)}")
                        print(f"Page ID: {change.get('id', 'N/A')}")
                        print(f"Server Name: {change.get('server_name', 'N/A')}")
                        
                        # Optional: Print raw JSON for debugging (commented out by default)
                        # print(f"\nRaw JSON:")
                        # print(json.dumps(change, indent=2)[:500] + "...")
                        
                        print()
                        
                        if events_read >= max_events:
                            print("=" * 80)
                            print(f"✓ Successfully read {max_events} page edit events")
                            print("=" * 80)
                            break
                            
                    except json.JSONDecodeError as e:
                        print(f"Warning: Failed to parse JSON: {e}")
                        continue
                    except KeyError as e:
                        print(f"Warning: Missing expected field: {e}")
                        continue
                        
    except ConnectionError as e:
        print(f"\n✗ Connection Error: Failed to connect to Wikipedia stream")
        print(f"  Details: {str(e)[:200]}")
        sys.exit(1)
    except TimeoutError as e:
        print(f"\n✗ Timeout Error: Connection timed out")
        print(f"  Details: {str(e)[:200]}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected Error: {type(e).__name__}")
        print(f"  Details: {str(e)[:200]}")
        sys.exit(1)
    finally:
        print("\nConnection test completed.")

def main():
    """
    Main function to run the Wikipedia changes stream connection test.
    """
    print("\n" + "=" * 80)
    print(" WIKIPEDIA CHANGES EVENT STREAM CONNECTION TEST")
    print("=" * 80)
    print("\nThis script will connect to the Wikipedia changes event stream")
    print("and read 10 sample page edit events with their metadata.\n")
    
    try:
        connect_and_read_wikipedia_changes()
        print("\n✓ Connection test successful!")
        
    except KeyboardInterrupt:
        print("\n\n⚠ Connection test interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Connection test failed: {str(e)[:200]}")
        sys.exit(1)

if __name__ == "__main__":
    main()