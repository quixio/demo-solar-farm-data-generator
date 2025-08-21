import os
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from quixstreams import Application
from quixstreams.sources import Source
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# for local dev, load env vars from a .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not required in production


class SlackSource(Source):
    """
    A Quix Streams Source that reads messages from a Slack channel using the Web API
    and publishes them to a Kafka topic.
    """

    def __init__(self, bot_token: str, channel_name: str, polling_interval: int = 60, max_messages: int = 100):
        super().__init__(name="slack-source")
        self.bot_token = bot_token
        self.channel_name = channel_name.lstrip('#')  # Remove # if present
        self.polling_interval = polling_interval
        self.max_messages = max_messages
        self.client = None
        self.channel_id = None
        self.last_timestamp = None
        self.processed_count = 0

    def _initialize_client(self):
        """Initialize Slack client and get channel ID."""
        try:
            print(f"🔗 Initializing Slack client for channel: #{self.channel_name}")
            self.client = WebClient(token=self.bot_token)
            
            # Test authentication
            auth_response = self.client.auth_test()
            if not auth_response['ok']:
                raise Exception(f"Authentication failed: {auth_response.get('error', 'Unknown error')}")
            
            print(f"✅ Authentication successful - Bot User: {auth_response.get('user', 'Unknown')}")
            
            # Get channel ID
            self.channel_id = self._get_channel_id()
            if not self.channel_id:
                raise Exception(f"Channel '#{self.channel_name}' not found or not accessible")
                
            print(f"✅ Connected to channel: #{self.channel_name} (ID: {self.channel_id})")
            
        except Exception as e:
            print(f"❌ Failed to initialize Slack client: {str(e)}")
            raise

    def _get_channel_id(self) -> Optional[str]:
        """Get channel ID from channel name."""
        try:
            response = self.client.conversations_list(
                types="public_channel,private_channel",
                limit=1000
            )
            
            channels = response.get('channels', [])
            for channel in channels:
                if channel['name'] == self.channel_name:
                    return channel['id']
            
            return None
            
        except SlackApiError as e:
            print(f"❌ Error getting channel list: {e.response['error']}")
            return None

    def _format_message(self, message: Dict) -> Dict:
        """
        Format a Slack message into the expected Kafka message format based on schema.
        """
        # Convert timestamp to readable format
        timestamp = float(message.get('ts', 0))
        readable_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') if timestamp else 'Unknown'
        
        # Format thread information
        thread_info = None
        if message.get('thread_ts'):
            thread_info = {
                "has_thread": True,
                "replies": message.get('reply_count', 0)
            }
        
        # Format files information
        files = []
        for file in message.get('files', []):
            files.append({
                "name": file.get('name', ''),
                "type": file.get('mimetype', '')
            })
        
        # Format reactions information
        reactions = []
        for reaction in message.get('reactions', []):
            reactions.append({
                "reaction": reaction.get('name', ''),
                "count": reaction.get('count', 0)
            })
        
        # Create the formatted message according to the schema
        formatted_message = {
            "id": message.get('ts', ''),
            "ts": message.get('ts', ''),
            "time": readable_time,
            "user_id": message.get('user', ''),
            "channel_id": self.channel_id,
            "type": message.get('type', 'message'),
            "text": message.get('text', ''),
        }
        
        # Add optional fields only if they exist
        if message.get('subtype'):
            formatted_message["subtype"] = message.get('subtype')
            
        if thread_info:
            formatted_message["thread"] = thread_info
            
        if files:
            formatted_message["files"] = files
            
        if reactions:
            formatted_message["reactions"] = reactions
            
        if message.get('inviter'):
            formatted_message["inviter"] = message.get('inviter')
        
        return formatted_message

    def _fetch_messages(self) -> List[Dict]:
        """Fetch new messages from the Slack channel."""
        try:
            # Build request parameters
            params = {
                "channel": self.channel_id,
                "limit": 100,  # Get up to 100 messages per request
                "include_all_metadata": True
            }
            
            # Only get messages after the last timestamp we processed
            if self.last_timestamp:
                params["oldest"] = self.last_timestamp
            
            print(f"📨 Fetching messages from #{self.channel_name}...")
            
            response = self.client.conversations_history(**params)
            
            if not response['ok']:
                print(f"❌ Error fetching messages: {response.get('error', 'Unknown error')}")
                return []
            
            messages = response.get('messages', [])
            
            # Filter out messages we've already processed
            if self.last_timestamp:
                messages = [msg for msg in messages if float(msg.get('ts', 0)) > float(self.last_timestamp)]
            
            # Sort messages by timestamp (oldest first)
            messages.sort(key=lambda x: float(x.get('ts', 0)))
            
            print(f"📬 Retrieved {len(messages)} new messages")
            return messages
            
        except SlackApiError as e:
            print(f"❌ Slack API Error: {e.response.get('error', 'Unknown error')}")
            return []
        except Exception as e:
            print(f"❌ Error fetching messages: {str(e)}")
            return []

    def run(self):
        """
        Main source execution loop. Fetches messages from Slack and produces them to Kafka.
        """
        try:
            # Initialize the Slack client
            self._initialize_client()
            
            print(f"🚀 Starting Slack source for channel #{self.channel_name}")
            print(f"📊 Max messages to process: {self.max_messages}")
            print(f"⏱️  Polling interval: {self.polling_interval} seconds")
            
            while self.running and self.processed_count < self.max_messages:
                try:
                    # Fetch new messages
                    messages = self._fetch_messages()
                    
                    if messages:
                        print(f"🔄 Processing {len(messages)} messages...")
                        
                        for message in messages:
                            # Check if we should stop
                            if not self.running or self.processed_count >= self.max_messages:
                                break
                                
                            # Format the message
                            formatted_message = self._format_message(message)
                            
                            # Print raw message structure for debugging (first few messages only)
                            if self.processed_count < 3:
                                print(f"🔍 Raw message structure: {json.dumps(message, indent=2, default=str)[:500]}...")
                                print(f"📝 Formatted message: {json.dumps(formatted_message, indent=2)}")
                            
                            # Serialize and produce the message
                            try:
                                # Use message ID as key
                                message_key = formatted_message.get('id', '')
                                serialized = self.serialize(key=message_key, value=formatted_message)
                                self.produce(key=serialized.key, value=serialized.value)
                                
                                print(f"✅ Produced message {self.processed_count + 1}: {formatted_message['text'][:100]}...")
                                
                                # Update tracking
                                self.last_timestamp = formatted_message['ts']
                                self.processed_count += 1
                                
                            except Exception as e:
                                print(f"❌ Error producing message: {str(e)}")
                                continue
                    else:
                        print("ℹ️  No new messages found")
                    
                    # Check if we've reached the limit
                    if self.processed_count >= self.max_messages:
                        print(f"🏁 Reached maximum message limit ({self.max_messages})")
                        break
                    
                    # Wait before next poll
                    if self.running:
                        print(f"⏸️  Waiting {self.polling_interval} seconds before next poll...")
                        time.sleep(self.polling_interval)
                        
                except KeyboardInterrupt:
                    print("⏹️  Stopping due to keyboard interrupt...")
                    break
                except Exception as e:
                    print(f"❌ Error in polling loop: {str(e)}")
                    if self.running:
                        print(f"⏸️  Retrying in {self.polling_interval} seconds...")
                        time.sleep(self.polling_interval)
            
            print(f"🏁 Slack source completed. Processed {self.processed_count} messages total.")
            
        except Exception as e:
            print(f"❌ Fatal error in Slack source: {str(e)}")
            raise


def main():
    """Setup and run the Slack source application."""
    print("🚀 Starting Slack to Kafka Source Application")
    print("=" * 60)
    
    # Get environment variables
    bot_token = os.getenv('SLACK_BOT_TOKEN')
    channel_name = os.getenv('CHANNEL_NAME', 'random')
    output_topic = os.getenv('output', 'slack-data')
    
    # Configuration
    polling_interval = int(os.getenv('POLLING_INTERVAL', '60'))  # seconds
    max_messages = int(os.getenv('MAX_MESSAGES', '100'))  # limit for testing
    
    if not bot_token:
        print("❌ Error: SLACK_BOT_TOKEN environment variable is required")
        print("   Please set your Slack Bot Token (starts with xoxb-)")
        return
    
    print(f"📝 Configuration:")
    print(f"   Channel: #{channel_name}")
    print(f"   Output Topic: {output_topic}")
    print(f"   Polling Interval: {polling_interval} seconds")
    print(f"   Max Messages (testing): {max_messages}")
    print(f"   Bot Token: xoxb-...{bot_token[-8:]}")  # Only show last 8 chars
    print()
    
    try:
        # Setup Quix Application
        app = Application(
            consumer_group="slack_data_producer",
            auto_create_topics=True
        )
        
        # Create the Slack source
        slack_source = SlackSource(
            bot_token=bot_token,
            channel_name=channel_name,
            polling_interval=polling_interval,
            max_messages=max_messages
        )
        
        # Create the output topic
        topic = app.topic(name=output_topic, value_serializer="json")
        
        # Add source to application
        app.add_source(source=slack_source, topic=topic)
        
        print("🎯 Pipeline configured. Starting application...")
        print("=" * 60)
        
        # Run the application
        app.run()
        
    except KeyboardInterrupt:
        print("\n⏹️  Application stopped by user")
    except Exception as e:
        print(f"\n❌ Application error: {str(e)}")
        raise


if __name__ == "__main__":
    main()