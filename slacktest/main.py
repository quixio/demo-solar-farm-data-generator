"""
Slack API Connection Test
This script tests the connection to a Slack workspace and retrieves sample messages
from a random channel using the Slack Web API.

This is a CONNECTION TEST ONLY - no Kafka/Quix integration yet.
"""

import os
import sys
import json
import random
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# for local dev, you can load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class SlackConnectionTester:
    """
    A connection tester for Slack API that retrieves sample messages 
    from a random channel in the workspace.
    """
    
    def __init__(self):
        """Initialize the Slack connection tester with credentials from environment."""
        # Get Slack tokens from environment variables
        self.bot_token = os.environ.get('SLACK_BOT_TOKEN')
        self.user_token = os.environ.get('SLACK_USER_TOKEN')
        
        # Validate required credentials
        if not self.bot_token:
            raise ValueError("SLACK_BOT_TOKEN environment variable is required")
        
        # Initialize Slack clients
        self.bot_client = WebClient(token=self.bot_token)
        self.user_client = WebClient(token=self.user_token) if self.user_token else None
        
        print(f"✅ Initialized Slack clients")
        print(f"   - Bot token: {'✅ Available' if self.bot_token else '❌ Missing'}")
        print(f"   - User token: {'✅ Available' if self.user_token else '⚠️ Optional - Not provided'}")

    def test_authentication(self) -> Dict[str, Any]:
        """Test authentication and get workspace information."""
        try:
            # Test bot token authentication
            print("\n🔐 Testing authentication...")
            
            bot_auth = self.bot_client.auth_test()
            
            if not bot_auth["ok"]:
                raise SlackApiError("Bot authentication failed", bot_auth)
            
            workspace_info = {
                "team_id": bot_auth["team_id"],
                "team": bot_auth["team"],
                "user_id": bot_auth["user_id"],
                "user": bot_auth["user"],
                "bot_id": bot_auth.get("bot_id")
            }
            
            print(f"✅ Authentication successful!")
            print(f"   - Workspace: {workspace_info['team']} (ID: {workspace_info['team_id']})")
            print(f"   - Bot User: {workspace_info['user']} (ID: {workspace_info['user_id']})")
            if workspace_info.get('bot_id'):
                print(f"   - Bot ID: {workspace_info['bot_id']}")
            
            return workspace_info
            
        except SlackApiError as e:
            print(f"❌ Authentication failed: {e.response['error']}")
            raise
        except Exception as e:
            print(f"❌ Unexpected error during authentication: {str(e)}")
            raise

    def get_public_channels(self) -> List[Dict[str, Any]]:
        """Retrieve list of public channels the bot can access."""
        try:
            print("\n📋 Retrieving public channels...")
            
            # Get list of public channels
            channels_response = self.bot_client.conversations_list(
                types="public_channel",
                exclude_archived=True,
                limit=100
            )
            
            if not channels_response["ok"]:
                raise SlackApiError("Failed to retrieve channels", channels_response)
            
            channels = channels_response["channels"]
            accessible_channels = []
            
            for channel in channels:
                # Filter for channels the bot is a member of or can read
                if channel.get("is_member") or not channel.get("is_private"):
                    accessible_channels.append({
                        "id": channel["id"],
                        "name": channel["name"],
                        "is_member": channel.get("is_member", False),
                        "num_members": channel.get("num_members", 0),
                        "purpose": channel.get("purpose", {}).get("value", ""),
                        "topic": channel.get("topic", {}).get("value", "")
                    })
            
            print(f"✅ Found {len(accessible_channels)} accessible public channels")
            if accessible_channels:
                print("   Sample channels:")
                for i, channel in enumerate(accessible_channels[:5]):  # Show first 5
                    print(f"     - #{channel['name']} ({channel['num_members']} members)")
            
            return accessible_channels
            
        except SlackApiError as e:
            print(f"❌ Failed to retrieve channels: {e.response['error']}")
            raise
        except Exception as e:
            print(f"❌ Unexpected error retrieving channels: {str(e)}")
            raise

    def select_random_channel(self, channels: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Select a random channel from the available channels."""
        if not channels:
            print("⚠️ No accessible channels found")
            return None
        
        # Prefer channels the bot is a member of
        member_channels = [ch for ch in channels if ch.get("is_member")]
        
        if member_channels:
            selected = random.choice(member_channels)
            print(f"📍 Randomly selected channel: #{selected['name']} (bot is member)")
        else:
            selected = random.choice(channels)
            print(f"📍 Randomly selected channel: #{selected['name']} (public access)")
        
        return selected

    def get_channel_messages(self, channel_id: str, channel_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Retrieve recent messages from a specific channel."""
        try:
            print(f"\n💬 Retrieving {limit} recent messages from #{channel_name}...")
            
            # Get conversation history
            history_response = self.bot_client.conversations_history(
                channel=channel_id,
                limit=limit,
                inclusive=True
            )
            
            if not history_response["ok"]:
                raise SlackApiError(f"Failed to retrieve messages from #{channel_name}", history_response)
            
            messages = history_response["messages"]
            processed_messages = []
            
            for i, msg in enumerate(messages, 1):
                # Process message data
                processed_msg = {
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "message_id": f"{channel_id}_{msg.get('ts', '')}",
                    "timestamp": msg.get("ts"),
                    "user_id": msg.get("user"),
                    "bot_id": msg.get("bot_id"),
                    "text": msg.get("text", ""),
                    "message_type": msg.get("type", "message"),
                    "subtype": msg.get("subtype"),
                    "thread_ts": msg.get("thread_ts"),
                    "reply_count": msg.get("reply_count", 0),
                    "reactions": msg.get("reactions", []),
                    "attachments": len(msg.get("attachments", [])),
                    "blocks": len(msg.get("blocks", [])),
                    "files": len(msg.get("files", [])),
                    "retrieved_at": datetime.utcnow().isoformat(),
                    "raw_timestamp": float(msg.get("ts", 0))
                }
                
                # Convert timestamp to readable format
                if processed_msg["timestamp"]:
                    try:
                        ts_float = float(processed_msg["timestamp"])
                        readable_time = datetime.fromtimestamp(ts_float).strftime("%Y-%m-%d %H:%M:%S UTC")
                        processed_msg["readable_timestamp"] = readable_time
                    except (ValueError, OSError):
                        processed_msg["readable_timestamp"] = "Invalid timestamp"
                
                processed_messages.append(processed_msg)
            
            print(f"✅ Retrieved {len(processed_messages)} messages from #{channel_name}")
            return processed_messages
            
        except SlackApiError as e:
            error_msg = e.response.get('error', 'Unknown error')
            if error_msg == "not_in_channel":
                print(f"⚠️ Bot is not a member of #{channel_name} - cannot retrieve messages")
                print("   Note: The bot needs to be added to private channels to read messages")
            elif error_msg == "channel_not_found":
                print(f"❌ Channel #{channel_name} not found")
            elif error_msg == "missing_scope":
                print(f"❌ Missing required OAuth scope to read messages from #{channel_name}")
                print("   Required scopes: channels:history (public channels) or groups:history (private channels)")
            else:
                print(f"❌ Failed to retrieve messages from #{channel_name}: {error_msg}")
            return []
        except Exception as e:
            print(f"❌ Unexpected error retrieving messages: {str(e)}")
            return []

    def display_sample_messages(self, messages: List[Dict[str, Any]]):
        """Display sample messages in a formatted way."""
        if not messages:
            print("\n📭 No messages to display")
            return
        
        print(f"\n📄 SAMPLE MESSAGES ({len(messages)} total):")
        print("=" * 80)
        
        for i, msg in enumerate(messages, 1):
            print(f"\n[Message {i}]")
            print(f"  Channel: #{msg['channel_name']}")
            print(f"  Time: {msg.get('readable_timestamp', 'N/A')}")
            print(f"  User ID: {msg.get('user_id', msg.get('bot_id', 'N/A'))}")
            print(f"  Type: {msg['message_type']}" + (f" ({msg['subtype']})" if msg['subtype'] else ""))
            
            # Display message text (truncate if very long)
            text = msg['text'][:200] + "..." if len(msg['text']) > 200 else msg['text']
            print(f"  Text: {text}")
            
            # Show additional metadata if present
            extras = []
            if msg['thread_ts']:
                extras.append(f"Thread reply")
            if msg['reply_count'] > 0:
                extras.append(f"{msg['reply_count']} replies")
            if msg['reactions']:
                extras.append(f"{len(msg['reactions'])} reactions")
            if msg['attachments'] > 0:
                extras.append(f"{msg['attachments']} attachments")
            if msg['files'] > 0:
                extras.append(f"{msg['files']} files")
            
            if extras:
                print(f"  Extras: {', '.join(extras)}")
            
            print("-" * 40)

    def get_workspace_stats(self) -> Dict[str, Any]:
        """Get additional workspace statistics."""
        stats = {}
        try:
            # Get team info
            team_info = self.bot_client.team_info()
            if team_info["ok"]:
                team = team_info["team"]
                stats["team_name"] = team["name"]
                stats["team_domain"] = team["domain"]
                
            return stats
        except:
            return {}

def main():
    """Main function to test Slack API connection."""
    print("🚀 SLACK API CONNECTION TEST")
    print("=" * 50)
    
    try:
        # Initialize connection tester
        tester = SlackConnectionTester()
        
        # Test authentication
        workspace_info = tester.test_authentication()
        
        # Get additional workspace stats
        stats = tester.get_workspace_stats()
        if stats:
            print(f"   - Domain: {stats.get('team_domain', 'N/A')}")
        
        # Get list of accessible channels
        channels = tester.get_public_channels()
        
        if not channels:
            print("\n⚠️ No accessible channels found.")
            print("   Make sure the bot is added to at least one public channel")
            print("   or has appropriate permissions to read channel lists.")
            return
        
        # Select a random channel
        selected_channel = tester.select_random_channel(channels)
        
        if not selected_channel:
            print("❌ Could not select a channel for testing")
            return
        
        # Get sample messages from the selected channel
        messages = tester.get_channel_messages(
            selected_channel["id"], 
            selected_channel["name"], 
            limit=10
        )
        
        # Display the results
        tester.display_sample_messages(messages)
        
        # Final summary
        print(f"\n📊 CONNECTION TEST SUMMARY:")
        print(f"   - Workspace: {workspace_info['team']} ({workspace_info['team_id']})")
        print(f"   - Total accessible channels: {len(channels)}")
        print(f"   - Tested channel: #{selected_channel['name']}")
        print(f"   - Messages retrieved: {len(messages)}")
        print(f"   - Test completed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        if messages:
            print("\n✅ CONNECTION TEST SUCCESSFUL!")
            print("   The bot can successfully connect to Slack and retrieve messages.")
        else:
            print("\n⚠️ CONNECTION TEST PARTIALLY SUCCESSFUL")
            print("   The bot can connect to Slack but could not retrieve messages.")
            print("   This may be due to permissions or channel membership issues.")
        
        return messages
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ CONNECTION TEST FAILED: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()