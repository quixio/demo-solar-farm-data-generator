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
    auth_token = os.environ.get('GITHUB_AUTH_TOKEN', '')
    timeout = int(os.environ.get('EVENT_STREAM_TIMEOUT', '30'))
    repo_filter = os.environ.get('GITHUB_REPO_FILTER', 'all-events')
    
    # Construct the full URL
    full_url = f"{api_url.rstrip('/')}{sse_path}"
    
    print(f"Connecting to GitHub SSE stream: {full_url}")
    print(f"Repository filter: {repo_filter}")
    print(f"Timeout: {timeout} seconds")
    print("-" * 50)
    
    # Setup headers
    headers = {
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'User-Agent': 'GitHub-SSE-Test-Client/1.0'
    }
    
    # Add authentication if token is provided
    if auth_token:
        headers['Authorization'] = f'token {auth_token}'
        print("Using authentication token")
    else:
        print("No authentication token provided")
    
    try:
        # Create SSE client with timeout
        print("Establishing SSE connection...")
        
        response = requests.get(
            full_url,
            headers=headers,
            stream=True,
            timeout=timeout
        )
        
        # Check if request was successful
        response.raise_for_status()
        print(f"Connection established successfully (Status: {response.status_code})")
        print("-" * 50)
        
        # Create SSE client from response
        client = SSEClient(response)
        
        event_count = 0
        target_count = 10
        
        print(f"Reading {target_count} sample events from stream:")
        print("=" * 50)
        
        for event in client.events():
            if event_count >= target_count:
                break
                
            event_count += 1
            
            print(f"\n--- Event {event_count} ---")
            print(f"Event Type: {event.event if event.event else 'message'}")
            print(f"Event ID: {event.id if event.id else 'N/A'}")
            
            # Try to parse event data as JSON for better formatting
            try:
                if event.data:
                    event_data = json.loads(event.data)
                    print("Event Data:")
                    print(json.dumps(event_data, indent=2))
                else:
                    print("Event Data: (empty)")
            except json.JSONDecodeError:
                print(f"Event Data (raw): {event.data}")
            except Exception as e:
                print(f"Error parsing event data: {e}")
                print(f"Raw data: {event.data}")
        
        print("\n" + "=" * 50)
        print(f"Successfully read {event_count} events from GitHub SSE stream")
        
    except requests.exceptions.Timeout:
        print(f"ERROR: Connection timed out after {timeout} seconds")
        print("This might indicate the stream is not available or network issues")
        
    except requests.exceptions.ConnectionError as e:
        print(f"ERROR: Connection failed - {e}")
        print("Please check the API URL and network connectivity")
        
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: HTTP error occurred - {e}")
        if response.status_code == 401:
            print("Authentication failed - check your GitHub token")
        elif response.status_code == 403:
            print("Access forbidden - check your permissions")
        elif response.status_code == 404:
            print("Endpoint not found - check the API URL and path")
        
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Request failed - {e}")
        
    except KeyboardInterrupt:
        print("\nConnection test interrupted by user")
        
    except Exception as e:
        print(f"ERROR: Unexpected error occurred - {e}")
        
    finally:
        print("\nConnection test completed")

if __name__ == "__main__":
    test_github_sse_connection()