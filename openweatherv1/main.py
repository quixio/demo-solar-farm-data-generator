#!/usr/bin/env python3
"""
OpenWeather API Quix Streams Source Application

This application connects to the OpenWeather API to fetch weather data for Berlin
and publishes it to a Kafka topic using Quix Streams.

Features:
- Fetches current weather data from OpenWeather API
- Transforms data into structured format
- Publishes to Kafka topic with proper error handling
- Includes retry logic and rate limiting
"""

import os
import time
import json
import requests
from datetime import datetime
from typing import Dict, Any, Optional
from quixstreams import Application
from quixstreams.sources import Source

# for local dev, load env vars from a .env file
# from dotenv import load_dotenv
# load_dotenv()


class OpenWeatherSource(Source):
    """
    A Quix Streams Source that fetches weather data from OpenWeather API.
    
    This source fetches weather data at regular intervals and publishes
    it to a Kafka topic for further processing.
    """
    
    def __init__(self, api_key: str, city_name: str, polling_interval: int = 300, *args, **kwargs):
        """
        Initialize the OpenWeather source.
        
        Args:
            api_key: OpenWeather API key
            city_name: Name of the city to get weather data for
            polling_interval: Interval in seconds between API calls (default: 5 minutes)
        """
        super().__init__(*args, **kwargs)
        self.api_key = api_key
        self.city_name = city_name
        self.polling_interval = polling_interval
        self.base_url = "http://api.openweathermap.org/data/2.5"
        self.message_count = 0
        self.max_messages = 100  # Limit for testing
        
        print(f"🌤️  OpenWeather Source initialized")
        print(f"📍 City: {city_name}")
        print(f"⏰ Polling interval: {polling_interval} seconds")
        print(f"🔢 Max messages for testing: {self.max_messages}")

    def test_connection(self) -> bool:
        """
        Test the API connection by making a simple request.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            print("🔍 Testing OpenWeather API connection...")
            
            url = f"{self.base_url}/weather"
            params = {
                'q': self.city_name,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                print("✅ Connection successful!")
                data = response.json()
                print(f"📍 Connected to weather data for: {data['name']}, {data['sys']['country']}")
                return True
            elif response.status_code == 401:
                print("❌ Authentication failed. Please check your API key.")
                return False
            elif response.status_code == 404:
                print(f"❌ City '{self.city_name}' not found. Please check the city name.")
                return False
            else:
                print(f"❌ Connection failed with status code: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Connection test error: {str(e)}")
            return False

    def get_current_weather(self) -> Optional[Dict[str, Any]]:
        """
        Get current weather data for the configured city.
        
        Returns:
            Weather data dictionary or None if failed
        """
        try:
            url = f"{self.base_url}/weather"
            params = {
                'q': self.city_name,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                raw_data = response.json()
                print(f"📡 Raw API response structure: {list(raw_data.keys())}")
                return raw_data
            else:
                print(f"⚠️ API request failed with status {response.status_code}: {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            print("⚠️ Request timeout - retrying in next cycle")
            return None
        except requests.exceptions.ConnectionError:
            print("⚠️ Connection error - retrying in next cycle")
            return None
        except Exception as e:
            print(f"⚠️ Error getting weather data: {str(e)}")
            return None

    def transform_weather_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw OpenWeather API response into structured format.
        
        Args:
            raw_data: Raw API response from OpenWeather
            
        Returns:
            Transformed weather data dictionary
        """
        try:
            # Extract key information with safe defaults
            main_data = raw_data.get('main', {})
            weather_data = raw_data.get('weather', [{}])[0]
            wind_data = raw_data.get('wind', {})
            clouds_data = raw_data.get('clouds', {})
            sys_data = raw_data.get('sys', {})
            coord_data = raw_data.get('coord', {})
            
            transformed = {
                # Metadata
                "source": "openweather_api",
                "data_type": "current_weather",
                "timestamp": datetime.utcnow().isoformat(),
                "api_timestamp": datetime.fromtimestamp(raw_data.get('dt', 0)).isoformat() if raw_data.get('dt') else None,
                
                # Location information
                "city": raw_data.get('name', 'Unknown'),
                "country": sys_data.get('country', 'Unknown'),
                "coordinates": {
                    "latitude": coord_data.get('lat', 0.0),
                    "longitude": coord_data.get('lon', 0.0)
                },
                "timezone": raw_data.get('timezone', 0),
                
                # Weather conditions
                "weather": {
                    "main": weather_data.get('main', 'Unknown'),
                    "description": weather_data.get('description', 'Unknown'),
                    "icon": weather_data.get('icon', ''),
                    "id": weather_data.get('id', 0)
                },
                
                # Temperature data
                "temperature": {
                    "current": main_data.get('temp', 0.0),
                    "feels_like": main_data.get('feels_like', 0.0),
                    "min": main_data.get('temp_min', 0.0),
                    "max": main_data.get('temp_max', 0.0)
                },
                
                # Atmospheric conditions
                "pressure": main_data.get('pressure', 0),
                "humidity": main_data.get('humidity', 0),
                "sea_level_pressure": main_data.get('sea_level', 0),
                "ground_level_pressure": main_data.get('grnd_level', 0),
                
                # Wind information
                "wind": {
                    "speed": wind_data.get('speed', 0.0),
                    "direction": wind_data.get('deg', 0),
                    "gust": wind_data.get('gust', 0.0)
                },
                
                # Cloud and visibility
                "cloudiness": clouds_data.get('all', 0),
                "visibility": raw_data.get('visibility', 0),
                
                # Sun times
                "sunrise": datetime.fromtimestamp(sys_data.get('sunrise', 0)).isoformat() if sys_data.get('sunrise') else None,
                "sunset": datetime.fromtimestamp(sys_data.get('sunset', 0)).isoformat() if sys_data.get('sunset') else None,
                
                # Additional data
                "rain": raw_data.get('rain', {}),
                "snow": raw_data.get('snow', {}),
            }
            
            print(f"🔄 Transformed data structure: {list(transformed.keys())}")
            return transformed
            
        except Exception as e:
            print(f"❌ Error transforming weather data: {str(e)}")
            # Return minimal structure on error
            return {
                "source": "openweather_api",
                "data_type": "current_weather",
                "timestamp": datetime.utcnow().isoformat(),
                "error": f"Transformation failed: {str(e)}",
                "raw_data": raw_data
            }

    def run(self):
        """
        Main execution loop for the source.
        
        This method runs continuously, fetching weather data at regular intervals
        and producing messages to the Kafka topic.
        """
        print(f"🚀 Starting OpenWeather source for {self.city_name}")
        
        # Test connection first
        if not self.test_connection():
            print("❌ Initial connection test failed. Exiting.")
            return
        
        print(f"📊 Starting data collection (max {self.max_messages} messages)...")
        
        while self.running and self.message_count < self.max_messages:
            print("I AM A TEST MESSAGE")
            try:
                print(f"\n🔄 Fetching weather data (message {self.message_count + 1}/{self.max_messages})...")
                
                # Get current weather data
                raw_weather_data = self.get_current_weather()
                
                if raw_weather_data:
                    # Transform the data
                    transformed_data = self.transform_weather_data(raw_weather_data)
                    
                    # Create message key (city name)
                    message_key = f"{transformed_data.get('city', 'unknown')}-{transformed_data.get('country', 'unknown')}"
                    
                    # Serialize and produce the message
                    try:
                        serialized_message = self.serialize(
                            key=message_key,
                            value=transformed_data
                        )
                        
                        self.produce(
                            key=serialized_message.key,
                            value=serialized_message.value
                        )
                        
                        self.message_count += 1
                        
                        print(f"✅ Message {self.message_count} published successfully!")
                        print(f"   City: {transformed_data.get('city')}")
                        print(f"   Temperature: {transformed_data.get('temperature', {}).get('current')}°C")
                        print(f"   Weather: {transformed_data.get('weather', {}).get('description')}")
                        
                    except Exception as e:
                        print(f"❌ Error producing message: {str(e)}")
                        
                else:
                    print("⚠️ No weather data received - skipping this cycle")
                
                # Wait for next polling interval (unless we've reached the limit)
                if self.message_count < self.max_messages:
                    print(f"⏳ Waiting {self.polling_interval} seconds until next poll...")
                    time.sleep(self.polling_interval)
                    
            except KeyboardInterrupt:
                print("\n⚠️ Received interrupt signal - stopping gracefully...")
                break
            except Exception as e:
                print(f"❌ Unexpected error in source loop: {str(e)}")
                print("⏳ Waiting 30 seconds before retry...")
                time.sleep(30)
        
        print(f"\n🏁 Source completed! Published {self.message_count} messages total.")


def main():
    """Main execution function."""
    
    print("🌤️  OpenWeather API Quix Streams Source")
    print("=" * 60)
    
    # Get environment variables
    api_key = os.environ.get('OPENWEATHER_API_KEY')
    city_name = os.environ.get('CITY_NAME', 'Berlin')
    output_topic = os.environ.get('output', 'openweather-data')
    
    if not api_key:
        print("❌ Error: OPENWEATHER_API_KEY environment variable not set")
        print("Please set your OpenWeather API key in the environment variables.")
        print("You can get a free API key at: https://openweathermap.org/api")
        return
    
    print(f"📍 Target City: {city_name}")
    print(f"🔑 API Key: {'*' * (len(api_key) - 4)}{api_key[-4:] if len(api_key) > 4 else '****'}")
    print(f"📤 Output Topic: {output_topic}")
    
    try:
        # Setup Quix Streams Application
        app = Application(consumer_group="openweather_producer", auto_create_topics=True)
        
        # Create the weather source
        weather_source = OpenWeatherSource(
            name="openweather-source",
            api_key=api_key,
            city_name=city_name,
            polling_interval=60  # Poll every minute for testing
        )
        
        # Setup output topic
        topic = app.topic(name=output_topic)
        
        # Option 1: Simple source addition (no additional processing)
        app.add_source(source=weather_source, topic=topic)
        
        # Option 2: Add processing with StreamingDataFrame (currently commented out)
        # Use this if you need to add transformations, filtering, etc.
        # sdf = app.dataframe(source=weather_source)
        # sdf.print(metadata=True)  # Debug print to see messages
        # sdf.to_topic(topic=topic)
        
        print(f"\n🏃 Running OpenWeather source application...")
        print("Press Ctrl+C to stop gracefully")
        
        # Run the application
        app.run()
        
    except KeyboardInterrupt:
        print("\n⚠️ Application interrupted by user.")
    except Exception as e:
        print(f"\n❌ Application error: {str(e)}")
        raise


# Sources require execution under a conditional main
if __name__ == "__main__":
    main()