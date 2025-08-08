# DEPENDENCIES:
# pip install requests
# pip install sseclient-py
# END_DEPENDENCIES

import os
import json
import requests
from sseclient import SSEClient

def test_github_sse_connection():
    """
    Test connection to GitHub public timeline SSE event stream.
    Reads exactly 10 sample events and prints them to console.
    """
    
    # Get configuration from environment variables
    api_url = os.environ.get('GITHUB_API_URL', 'http://github-firehose.libraries.io/events')
    sse_path = os.environ.get('GITHUB_SSE_PATH', '/events')
    timeout = int(os.environ.get('EVENT_STREAM_TIMEOUT', '30'))
    repo_filter = os.environ.get('GITHUB_REPO_FILTER', 'all-events')
    
    # Construct the full URL
    full_url = api_url.rstrip('/') + sse_path
    
    print(f"Connecting to GitHub SSE stream: {full_url}")
    print(f"Timeout: {timeout} seconds")
    print(f"Repository filter: {repo_filter}")
    print("-" * 50)
    
    try:
        # Set up headers for SSE connection
        headers = {
            'Accept': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'User-Agent': 'GitHub-SSE-Test-Client/1.0'
        }
        
        # Create SSE client with timeout
        response = requests.get(full_url, headers=headers, stream=True, timeout=timeout)
        response.raise_for_status()
        
        print(f"Successfully connected to SSE stream (Status: {response.status_code})")
        print("Reading 10 sample events...\n")
        
        # Create SSE client from response
        client = SSEClient(response)
        
        event_count = 0
        target_count = 10
        
        for event in client.events():
            if event_count >= target_count:
                break
                
            event_count += 1
            
            print(f"Event #{event_count}:")
            print(f"  Type: {event.event if event.event else 'message'}")
            print(f"  ID: {event.id if event.id else 'N/A'}")
            
            # Try to parse event data as JSON for better formatting
            try:
                if event.data:
                    event_data = json.loads(event.data)
                    print(f"  Data: {json.dumps(event_data, indent=2)}")
                else:
                    print("  Data: (empty)")
            except json.JSONDecodeError:
                # If not JSON, print raw data
                print(f"  Data: {event.data}")
            
            print("-" * 30)
        
        print(f"\nSuccessfully read {event_count} events from GitHub SSE stream")
        
    except requests.exceptions.Timeout:
        print(f"ERROR: Connection timed out after {timeout} seconds")
        print("This might indicate the SSE stream is not responding or is very slow")
        
    except requests.exceptions.ConnectionError as e:
        print(f"ERROR: Failed to connect to SSE stream: {e}")
        print("Please check the GITHUB_API_URL and network connectivity")
        
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: HTTP error occurred: {e}")
        print(f"Response status: {response.status_code}")
        
    except KeyboardInterrupt:
        print("\nConnection test interrupted by user")
        
    except Exception as e:
        print(f"ERROR: Unexpected error occurred: {e}")
        
    finally:
        print("\nConnection test completed")

if __name__ == "__main__":
    test_github_sse_connection()