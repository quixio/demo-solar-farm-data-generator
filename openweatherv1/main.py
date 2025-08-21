#!/usr/bin/env python3
"""
OpenWeather API Connection Test

This script tests the connection to OpenWeather API by fetching current weather
data for a specified city. It retrieves exactly 10 sample weather data points
(current conditions, forecast data, or multiple cities) and displays them in
a clear, formatted manner.

This is a connection test only - no Kafka integration.
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional


class OpenWeatherConnector:
    """OpenWeather API connection and data retrieval handler."""
    
    def __init__(self, api_key: str, city_name: str):
        """
        Initialize the OpenWeather connector.
        
        Args:
            api_key: OpenWeather API key
            city_name: Name of the city to get weather data for
        """
        self.api_key = api_key
        self.city_name = city_name
        self.base_url = "http://api.openweathermap.org/data/2.5"
        self.geo_url = "http://api.openweathermap.org/geo/1.0"
        
    def test_connection(self) -> bool:
        """
        Test the API connection by making a simple request.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            print("🔍 Testing OpenWeather API connection...")
            
            # Test with a simple current weather request
            test_url = f"{self.base_url}/weather"
            params = {
                'q': self.city_name,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(test_url, params=params, timeout=10)
            
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
                
        except requests.exceptions.Timeout:
            print("❌ Connection timeout. Please check your internet connection.")
            return False
        except requests.exceptions.ConnectionError:
            print("❌ Connection error. Please check your internet connection.")
            return False
        except Exception as e:
            print(f"❌ Unexpected error during connection test: {str(e)}")
            return False
    
    def get_coordinates(self, city_name: str) -> Optional[tuple]:
        """
        Get coordinates for a city name.
        
        Args:
            city_name: Name of the city
            
        Returns:
            Tuple of (lat, lon) or None if not found
        """
        try:
            url = f"{self.geo_url}/direct"
            params = {
                'q': city_name,
                'limit': 1,
                'appid': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data:
                    return (data[0]['lat'], data[0]['lon'])
            return None
            
        except Exception as e:
            print(f"⚠️ Error getting coordinates for {city_name}: {str(e)}")
            return None
    
    def get_current_weather(self, city_name: str) -> Optional[Dict[str, Any]]:
        """
        Get current weather data for a city.
        
        Args:
            city_name: Name of the city
            
        Returns:
            Weather data dictionary or None if failed
        """
        try:
            url = f"{self.base_url}/weather"
            params = {
                'q': city_name,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️ Failed to get weather for {city_name}: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"⚠️ Error getting weather for {city_name}: {str(e)}")
            return None
    
    def get_forecast_weather(self, city_name: str) -> Optional[Dict[str, Any]]:
        """
        Get 5-day weather forecast data for a city.
        
        Args:
            city_name: Name of the city
            
        Returns:
            Forecast data dictionary or None if failed
        """
        try:
            url = f"{self.base_url}/forecast"
            params = {
                'q': city_name,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️ Failed to get forecast for {city_name}: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"⚠️ Error getting forecast for {city_name}: {str(e)}")
            return None
    
    def collect_sample_data(self) -> List[Dict[str, Any]]:
        """
        Collect exactly 10 sample weather data points.
        
        Returns:
            List of weather data dictionaries
        """
        samples = []
        print(f"\n📊 Collecting 10 sample weather data points...")
        
        # 1. Get current weather for the main city
        print("1️⃣ Fetching current weather data...")
        current_weather = self.get_current_weather(self.city_name)
        if current_weather:
            sample = {
                "type": "current_weather",
                "city": current_weather['name'],
                "country": current_weather['sys']['country'],
                "timestamp": datetime.fromtimestamp(current_weather['dt']).isoformat(),
                "temperature": current_weather['main']['temp'],
                "feels_like": current_weather['main']['feels_like'],
                "humidity": current_weather['main']['humidity'],
                "pressure": current_weather['main']['pressure'],
                "weather_main": current_weather['weather'][0]['main'],
                "weather_description": current_weather['weather'][0]['description'],
                "wind_speed": current_weather.get('wind', {}).get('speed', 0),
                "wind_direction": current_weather.get('wind', {}).get('deg', 0),
                "cloudiness": current_weather['clouds']['all'],
                "visibility": current_weather.get('visibility', 0),
                "coordinates": {
                    "lat": current_weather['coord']['lat'],
                    "lon": current_weather['coord']['lon']
                }
            }
            samples.append(sample)
            print(f"   ✅ Current weather for {current_weather['name']}: {current_weather['main']['temp']}°C")
        
        # 2-6. Get forecast data (next 5 forecast points)
        print("2️⃣-6️⃣ Fetching forecast data...")
        forecast_data = self.get_forecast_weather(self.city_name)
        if forecast_data and 'list' in forecast_data:
            for i, forecast in enumerate(forecast_data['list'][:5]):
                sample = {
                    "type": "forecast",
                    "city": forecast_data['city']['name'],
                    "country": forecast_data['city']['country'],
                    "timestamp": datetime.fromtimestamp(forecast['dt']).isoformat(),
                    "forecast_time": f"+{(i+1)*3}h",  # Forecasts are usually 3-hour intervals
                    "temperature": forecast['main']['temp'],
                    "feels_like": forecast['main']['feels_like'],
                    "humidity": forecast['main']['humidity'],
                    "pressure": forecast['main']['pressure'],
                    "weather_main": forecast['weather'][0]['main'],
                    "weather_description": forecast['weather'][0]['description'],
                    "wind_speed": forecast.get('wind', {}).get('speed', 0),
                    "wind_direction": forecast.get('wind', {}).get('deg', 0),
                    "cloudiness": forecast['clouds']['all'],
                    "pop": forecast.get('pop', 0) * 100,  # Probability of precipitation
                    "coordinates": {
                        "lat": forecast_data['city']['coord']['lat'],
                        "lon": forecast_data['city']['coord']['lon']
                    }
                }
                samples.append(sample)
                print(f"   ✅ Forecast {forecast['dt_txt']}: {forecast['main']['temp']}°C")
        
        # 7-10. Get current weather for nearby cities
        nearby_cities = ["Munich", "Hamburg", "Frankfurt", "Dresden"]
        print("7️⃣-🔟 Fetching weather for nearby cities...")
        
        for i, city in enumerate(nearby_cities):
            if len(samples) >= 10:
                break
                
            city_weather = self.get_current_weather(city)
            if city_weather:
                sample = {
                    "type": "nearby_city",
                    "city": city_weather['name'],
                    "country": city_weather['sys']['country'],
                    "timestamp": datetime.fromtimestamp(city_weather['dt']).isoformat(),
                    "temperature": city_weather['main']['temp'],
                    "feels_like": city_weather['main']['feels_like'],
                    "humidity": city_weather['main']['humidity'],
                    "pressure": city_weather['main']['pressure'],
                    "weather_main": city_weather['weather'][0]['main'],
                    "weather_description": city_weather['weather'][0]['description'],
                    "wind_speed": city_weather.get('wind', {}).get('speed', 0),
                    "wind_direction": city_weather.get('wind', {}).get('deg', 0),
                    "cloudiness": city_weather['clouds']['all'],
                    "visibility": city_weather.get('visibility', 0),
                    "coordinates": {
                        "lat": city_weather['coord']['lat'],
                        "lon": city_weather['coord']['lon']
                    }
                }
                samples.append(sample)
                print(f"   ✅ Weather for {city_weather['name']}: {city_weather['main']['temp']}°C")
            
            # Add small delay to respect rate limits
            time.sleep(0.2)
        
        return samples[:10]  # Ensure exactly 10 samples
    
    def format_sample_data(self, samples: List[Dict[str, Any]]) -> None:
        """
        Format and display sample data in a readable format.
        
        Args:
            samples: List of weather data samples
        """
        print(f"\n" + "="*80)
        print(f"📋 WEATHER DATA SAMPLES ({len(samples)} items)")
        print("="*80)
        
        for i, sample in enumerate(samples, 1):
            print(f"\n🌤️  SAMPLE {i}: {sample['type'].upper()}")
            print("-" * 40)
            print(f"Location: {sample['city']}, {sample['country']}")
            print(f"Coordinates: {sample['coordinates']['lat']:.4f}, {sample['coordinates']['lon']:.4f}")
            print(f"Timestamp: {sample['timestamp']}")
            
            if 'forecast_time' in sample:
                print(f"Forecast: {sample['forecast_time']} from now")
            
            print(f"Temperature: {sample['temperature']:.1f}°C (feels like {sample['feels_like']:.1f}°C)")
            print(f"Weather: {sample['weather_main']} - {sample['weather_description'].title()}")
            print(f"Humidity: {sample['humidity']}%")
            print(f"Pressure: {sample['pressure']} hPa")
            print(f"Wind: {sample['wind_speed']:.1f} m/s @ {sample['wind_direction']}°")
            print(f"Cloudiness: {sample['cloudiness']}%")
            
            if 'visibility' in sample and sample['visibility'] > 0:
                print(f"Visibility: {sample['visibility']/1000:.1f} km")
            
            if 'pop' in sample:
                print(f"Precipitation Probability: {sample['pop']:.0f}%")
    
    def get_api_limits_info(self) -> Dict[str, Any]:
        """
        Get information about API limits and usage.
        
        Returns:
            Dictionary with API limits information
        """
        # OpenWeather free tier: 1000 calls/day, 60 calls/minute
        return {
            "free_tier_daily_limit": 1000,
            "free_tier_minute_limit": 60,
            "recommended_interval_seconds": 60,  # 1 call per minute for safety
            "endpoints_used": [
                "Current Weather Data",
                "5 Day Weather Forecast",
                "Geocoding (Direct)"
            ],
            "data_update_frequency": "10 minutes",
            "note": "This test uses the free tier. For production use, consider upgrading to a paid plan."
        }


def main():
    """Main execution function for the connection test."""
    
    print("🌤️  OpenWeather API Connection Test")
    print("=" * 50)
    
    # Get environment variables
    api_key = os.environ.get('OPENWEATHER_API_KEY')
    city_name = os.environ.get('CITY_NAME', 'Berlin')
    
    if not api_key:
        print("❌ Error: OPENWEATHER_API_KEY environment variable not set")
        print("Please set your OpenWeather API key in the environment variables.")
        print("You can get a free API key at: https://openweathermap.org/api")
        sys.exit(1)
    
    print(f"📍 Target City: {city_name}")
    print(f"🔑 API Key: {'*' * (len(api_key) - 4)}{api_key[-4:] if len(api_key) > 4 else '****'}")
    
    try:
        # Initialize connector
        connector = OpenWeatherConnector(api_key, city_name)
        
        # Test connection
        if not connector.test_connection():
            print("\n❌ Connection test failed. Please check your configuration.")
            sys.exit(1)
        
        # Collect sample data
        samples = connector.collect_sample_data()
        
        if not samples:
            print("\n❌ No sample data collected. Please check your API key and internet connection.")
            sys.exit(1)
        
        # Display formatted results
        connector.format_sample_data(samples)
        
        # Display API information
        print(f"\n" + "="*80)
        print("📊 API LIMITS & INFORMATION")
        print("="*80)
        
        limits_info = connector.get_api_limits_info()
        for key, value in limits_info.items():
            if key == "endpoints_used":
                print(f"Endpoints Used: {', '.join(value)}")
            elif key == "note":
                print(f"Note: {value}")
            else:
                print(f"{key.replace('_', ' ').title()}: {value}")
        
        # Summary
        print(f"\n" + "="*80)
        print("✅ CONNECTION TEST COMPLETED SUCCESSFULLY!")
        print("="*80)
        print(f"• Successfully connected to OpenWeather API")
        print(f"• Retrieved {len(samples)} sample weather data points")
        print(f"• Primary city: {city_name}")
        print(f"• Data includes current weather, forecasts, and nearby cities")
        print(f"• All data is ready for Kafka integration in the next phase")
        
        # Data structure information for Kafka integration
        print(f"\n📝 DATA STRUCTURE FOR KAFKA INTEGRATION:")
        print("-" * 50)
        if samples:
            sample_keys = list(samples[0].keys())
            print("Key fields in each weather record:")
            for key in sample_keys:
                print(f"  • {key}")
        
        print(f"\n🚀 Next Steps:")
        print("1. Integrate this connector with Quix Streams")
        print("2. Set up Kafka topic for weather data")
        print("3. Add data transformation and filtering as needed")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Test interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        print("Please check your configuration and try again.")
        sys.exit(1)


if __name__ == "__main__":
    main()