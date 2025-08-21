import os
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def format_message(message: Dict) -> Dict:
    """
    Format a Slack message for display and future processing.
    
    Args:
        message: Raw message dict from Slack API
        
    Returns:
        Formatted message dict with standardized fields
    """
    # Convert timestamp to readable format
    timestamp = float(message.get('ts', 0))
    readable_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') if timestamp else 'Unknown'
    
    return {
        'message_id': message.get('ts', 'unknown'),
        'timestamp': timestamp,
        'readable_time': readable_time,
        'user_id': message.get('user', 'unknown'),
        'text': message.get('text', ''),
        'message_type': message.get('type', 'message'),
        'subtype': message.get('subtype'),
        'thread_ts': message.get('thread_ts'),
        'reply_count': message.get('reply_count', 0),
        'reactions': message.get('reactions', []),
        'files': [{'id': f.get('id'), 'name': f.get('name'), 'mimetype': f.get('mimetype')} 
                 for f in message.get('files', [])],
        'raw_message': message  # Keep original for debugging
    }

def get_channel_id(client: WebClient, channel_name: str) -> Optional[str]:
    """
    Get channel ID from channel name.
    
    Args:
        client: Slack WebClient instance
        channel_name: Name of the channel (without # prefix)
        
    Returns:
        Channel ID if found, None otherwise
    """
    try:
        # Remove # if present
        channel_name = channel_name.lstrip('#')
        
        print(f"🔍 Searching for channel: #{channel_name}")
        
        # Get list of channels
        response = client.conversations_list(
            types="public_channel,private_channel",
            limit=1000
        )
        
        channels = response.get('channels', [])
        print(f"📋 Found {len(channels)} total channels")
        
        # Find matching channel
        for channel in channels:
            if channel['name'] == channel_name:
                print(f"✅ Found channel: #{channel['name']} (ID: {channel['id']})")
                return channel['id']
        
        print(f"❌ Channel '#{channel_name}' not found")
        print("Available channels:")
        for channel in channels[:20]:  # Show first 20 channels
            print(f"  - #{channel['name']}")
        if len(channels) > 20:
            print(f"  ... and {len(channels) - 20} more channels")
            
        return None
        
    except SlackApiError as e:
        print(f"❌ Error getting channel list: {e.response['error']}")
        return None

def test_slack_connection():
    """
    Test connection to Slack and read messages from specified channel.
    """
    print("🚀 Starting Slack Connection Test")
    print("=" * 50)
    
    # Get environment variables
    bot_token = os.getenv('SLACK_BOT_TOKEN')
    channel_name = os.getenv('CHANNEL_NAME', 'random')
    
    if not bot_token:
        print("❌ Error: SLACK_BOT_TOKEN environment variable is required")
        print("   Please set your Slack Bot Token (starts with xoxb-)")
        return False
    
    print(f"📝 Configuration:")
    print(f"   Channel: #{channel_name}")
    print(f"   Bot Token: xoxb-...{bot_token[-8:]}")  # Only show last 8 chars for security
    print()
    
    try:
        # Initialize Slack client
        print("🔗 Initializing Slack client...")
        client = WebClient(token=bot_token)
        
        # Test authentication
        print("🔐 Testing authentication...")
        auth_response = client.auth_test()
        
        if not auth_response['ok']:
            print(f"❌ Authentication failed: {auth_response.get('error', 'Unknown error')}")
            return False
            
        print(f"✅ Authentication successful!")
        print(f"   Bot User: {auth_response.get('user', 'Unknown')}")
        print(f"   Team: {auth_response.get('team', 'Unknown')}")
        print(f"   User ID: {auth_response.get('user_id', 'Unknown')}")
        print()
        
        # Get channel ID
        channel_id = get_channel_id(client, channel_name)
        if not channel_id:
            return False
        
        print()
        print("📨 Fetching messages...")
        
        # Fetch messages from the channel
        response = client.conversations_history(
            channel=channel_id,
            limit=10,  # Get exactly 10 messages as requested
            include_all_metadata=True
        )
        
        if not response['ok']:
            print(f"❌ Error fetching messages: {response.get('error', 'Unknown error')}")
            return False
            
        messages = response.get('messages', [])
        print(f"📬 Retrieved {len(messages)} messages from #{channel_name}")
        print()
        
        if not messages:
            print("ℹ️  No messages found in this channel")
            return True
        
        # Display messages
        print("🔍 MESSAGE SAMPLES:")
        print("=" * 80)
        
        for i, message in enumerate(messages, 1):
            formatted_msg = format_message(message)
            
            print(f"\n📝 Message {i}:")
            print(f"   ID: {formatted_msg['message_id']}")
            print(f"   Time: {formatted_msg['readable_time']}")
            print(f"   User ID: {formatted_msg['user_id']}")
            print(f"   Type: {formatted_msg['message_type']}")
            if formatted_msg['subtype']:
                print(f"   Subtype: {formatted_msg['subtype']}")
            print(f"   Text: {formatted_msg['text'][:200]}{'...' if len(formatted_msg['text']) > 200 else ''}")
            
            if formatted_msg['files']:
                print(f"   Files: {len(formatted_msg['files'])} file(s)")
                for file in formatted_msg['files']:
                    print(f"     - {file['name']} ({file['mimetype']})")
            
            if formatted_msg['reactions']:
                reactions = ', '.join([f"{r['name']}({r['count']})" for r in formatted_msg['reactions']])
                print(f"   Reactions: {reactions}")
            
            if formatted_msg['thread_ts']:
                print(f"   Thread: Yes (replies: {formatted_msg['reply_count']})")
                
            print("-" * 80)
        
        # Display data structure summary
        print("\n📊 DATA STRUCTURE ANALYSIS:")
        print("=" * 80)
        
        if messages:
            sample_message = messages[0]
            print("🔍 Sample raw message structure:")
            print(json.dumps(sample_message, indent=2, default=str)[:1000] + "...")
            
            print(f"\n📋 Common fields found across messages:")
            common_fields = set()
            for msg in messages:
                common_fields.update(msg.keys())
            
            for field in sorted(common_fields):
                count = sum(1 for msg in messages if field in msg)
                print(f"   - {field}: present in {count}/{len(messages)} messages")
        
        print("\n✅ Connection test completed successfully!")
        print(f"📊 Summary: Retrieved {len(messages)} messages from #{channel_name}")
        
        return True
        
    except SlackApiError as e:
        error_message = e.response.get('error', 'Unknown error')
        print(f"❌ Slack API Error: {error_message}")
        
        if error_message == 'invalid_auth':
            print("   🔧 Solution: Check your SLACK_BOT_TOKEN is correct and starts with 'xoxb-'")
        elif error_message == 'channel_not_found':
            print(f"   🔧 Solution: Channel '#{channel_name}' doesn't exist or bot doesn't have access")
        elif error_message == 'not_in_channel':
            print(f"   🔧 Solution: Add your bot to the '#{channel_name}' channel")
        elif error_message == 'missing_scope':
            print("   🔧 Solution: Your bot token needs additional OAuth scopes")
            print("      Required scopes: channels:history, channels:read, groups:history, groups:read")
        
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        print(f"   Error type: {type(e).__name__}")
        return False

def main():
    """
    Main function - runs the connection test.
    This is a CONNECTION TEST ONLY - not integrated with Quix Streams yet.
    """
    print("🔧 SLACK CONNECTION TEST")
    print("This is a connection test only - no Kafka integration yet")
    print("=" * 60)
    
    success = test_slack_connection()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ CONNECTION TEST PASSED")
        print("   Ready for Quix Streams integration!")
    else:
        print("❌ CONNECTION TEST FAILED")
        print("   Please fix the issues above before proceeding.")
    
    print("=" * 60)

if __name__ == "__main__":
    # Load environment variables from .env if available (for local testing)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # dotenv not required in production
    
    main()