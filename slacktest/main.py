"""
Slack Connection Test
This script tests the connection to Slack Web API and retrieves sample messages 
from a specified channel for inspection. This is a connection test only - 
no Kafka integration is included yet.
"""

import os
import time
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


def get_channel_id(client, channel_name):
    """
    Get the channel ID from the channel name.
    
    Args:
        client: Slack WebClient instance
        channel_name: Name of the channel (without #)
    
    Returns:
        Channel ID if found, None otherwise
    """
    try:
        # Get list of conversations (channels)
        response = client.conversations_list(types="public_channel,private_channel")
        
        for channel in response["channels"]:
            if channel["name"] == channel_name:
                return channel["id"]
        
        print(f"❌ Channel '{channel_name}' not found")
        print("Available channels:")
        for channel in response["channels"][:10]:  # Show first 10 channels
            print(f"  - {channel['name']}")
        return None
        
    except SlackApiError as e:
        print(f"❌ Error getting channel list: {e.response['error']}")
        return None


def format_message(message, channel_name):
    """
    Format a Slack message for display.
    
    Args:
        message: Message object from Slack API
        channel_name: Name of the channel
    
    Returns:
        Formatted string representation
    """
    # Extract basic info
    user = message.get("user", "Unknown")
    text = message.get("text", "")
    timestamp = float(message.get("ts", 0))
    
    # Convert timestamp to readable format
    dt = datetime.fromtimestamp(timestamp)
    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Handle different message types
    msg_type = message.get("type", "message")
    subtype = message.get("subtype", "")
    
    formatted = f"""
📧 Message ID: {message.get('ts', 'N/A')}
👤 User: {user}
📅 Time: {formatted_time}
📍 Channel: #{channel_name}
💬 Text: {text[:200]}{'...' if len(text) > 200 else ''}
🏷️  Type: {msg_type}
{f'🔖 Subtype: {subtype}' if subtype else ''}
{'─' * 60}"""
    
    return formatted


def test_slack_connection():
    """
    Test connection to Slack and retrieve sample messages from the specified channel.
    """
    print("🚀 Starting Slack Connection Test...")
    print("=" * 60)
    
    # Get environment variables
    bot_token = os.environ.get("SLACK_BOT_TOKEN")
    channel_name = os.environ.get("SLACK_CHANNEL_NAME", "devrel-squad")
    
    if not bot_token:
        print("❌ Error: SLACK_BOT_TOKEN environment variable is required")
        print("   Please set it to your Slack Bot User OAuth Token (starts with xoxb-)")
        return False
    
    print(f"🔑 Using bot token: {bot_token[:12]}...")
    print(f"📢 Target channel: #{channel_name}")
    print()
    
    # Initialize Slack client
    client = WebClient(token=bot_token)
    
    try:
        # Test authentication
        print("🔐 Testing authentication...")
        auth_response = client.auth_test()
        print(f"✅ Authentication successful!")
        print(f"   Team: {auth_response['team']}")
        print(f"   User: {auth_response['user']}")
        print(f"   Bot ID: {auth_response.get('bot_id', 'N/A')}")
        print()
        
    except SlackApiError as e:
        print(f"❌ Authentication failed: {e.response['error']}")
        print("   Please check your SLACK_BOT_TOKEN")
        return False
    
    # Get channel ID
    print(f"🔍 Looking for channel '#{channel_name}'...")
    channel_id = get_channel_id(client, channel_name)
    
    if not channel_id:
        return False
    
    print(f"✅ Found channel ID: {channel_id}")
    print()
    
    # Retrieve messages
    try:
        print("📥 Retrieving messages from channel...")
        response = client.conversations_history(
            channel=channel_id,
            limit=10,  # Get exactly 10 messages
            include_all_metadata=True
        )
        
        messages = response["messages"]
        
        if not messages:
            print("⚠️  No messages found in the channel")
            return True
        
        print(f"✅ Retrieved {len(messages)} message(s)")
        print()
        print("📋 MESSAGE DETAILS:")
        print("=" * 60)
        
        # Display each message
        for i, message in enumerate(messages, 1):
            print(f"MESSAGE #{i}")
            print(format_message(message, channel_name))
        
        # Show sample data structure
        print()
        print("🔬 SAMPLE DATA STRUCTURE:")
        print("=" * 60)
        print("Keys available in message objects:")
        if messages:
            sample_msg = messages[0]
            for key in sorted(sample_msg.keys()):
                value = sample_msg[key]
                if isinstance(value, str) and len(value) > 50:
                    value = value[:50] + "..."
                print(f"  • {key}: {type(value).__name__} = {value}")
        
        # Show connection metadata
        print()
        print("📊 CONNECTION METADATA:")
        print("=" * 60)
        print(f"• Total messages retrieved: {len(messages)}")
        print(f"• Channel ID: {channel_id}")
        print(f"• Channel name: #{channel_name}")
        print(f"• API response keys: {list(response.keys())}")
        
        print()
        print("✅ Connection test completed successfully!")
        print("   Ready to integrate with Quix Streams for data processing.")
        
        return True
        
    except SlackApiError as e:
        error_code = e.response['error']
        print(f"❌ Error retrieving messages: {error_code}")
        
        if error_code == "channel_not_found":
            print("   The channel was not found or the bot doesn't have access to it")
        elif error_code == "not_in_channel":
            print("   The bot needs to be added to the channel first")
        elif error_code == "missing_scope":
            print("   The bot token is missing required permissions")
            print("   Required scopes: channels:history, channels:read")
        else:
            print(f"   Check Slack API documentation for error code: {error_code}")
        
        return False
    
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False


def main():
    """
    Main function to run the Slack connection test.
    """
    try:
        success = test_slack_connection()
        if success:
            print("\n🎉 Test completed successfully!")
        else:
            print("\n💥 Test failed. Please check the error messages above.")
            exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error in main: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()