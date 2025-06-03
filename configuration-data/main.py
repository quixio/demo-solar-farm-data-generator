from quixstreams import Application
from quixstreams.sources import Source
import os
import random
import time
import math
from dataclasses import dataclass
from typing import Dict, List, Optional
import json

# Get location from environment variable
location = os.getenv("location", "LONDON").upper()

# Weather condition types
WEATHER_CONDITIONS = [
    "clear", "partly_cloudy", "cloudy", "overcast",
    "light_rain", "moderate_rain", "heavy_rain",
    "thunderstorm", "fog", "mist"
]

@dataclass
class WeatherForecast:
    """Represents a weather forecast data point."""
    timestamp: int
    location: str
    temperature: float  # Celsius
    feels_like: float  # Celsius
    humidity: float  # %
    cloud_cover: float  # %
    wind_speed: float  # m/s
    wind_direction: float  # degrees
    pressure: float  # hPa
    condition: str  # weather condition
    visibility: float  # km
    precipitation_prob: float  # %
    uv_index: float  # 0-11+
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp,
            "location": self.location,
            "temperature": round(self.temperature, 1),
            "feels_like": round(self.feels_like, 1),
            "humidity": round(self.humidity, 1),
            "cloud_cover": round(self.cloud_cover, 1),
            "wind_speed": round(self.wind_speed, 1),
            "wind_direction": int(self.wind_direction),
            "pressure": int(self.pressure),
            "condition": self.condition,
            "visibility": round(self.visibility, 1),
            "precipitation_prob": int(self.precipitation_prob),
            "uv_index": round(self.uv_index, 1)
        }


class WeatherForecastGenerator(Source):
    """
    A Quix Streams Source that generates realistic weather forecast data.
    Emits new weather data every 5 minutes.
    """
    
    def __init__(self, name: str, location: str):
        # Initialize base class
        Source.__init__(self, name)
        
        # Configuration
        self.location = location
        self.time_step = 5 * 60 * 1_000_000_000  # 5 minutes in nanoseconds
        self.current_time = int(time.time() * 1_000_000_000)  # Current time in ns
        
        # Weather state that changes slowly over time
        self.base_temperature = self._get_seasonal_base_temp()
        self.current_condition = random.choice(WEATHER_CONDITIONS)
        self.weather_trend = random.uniform(-0.5, 0.5)  # -0.5 (worsening) to 0.5 (improving)
        
    def _get_seasonal_base_temp(self) -> float:
        """Get base temperature based on current month."""
        month = time.localtime().tm_mon
        # Base temperatures in Celsius by month (for northern hemisphere)
        monthly_temps = [5, 5, 8, 12, 16, 19, 22, 21, 18, 14, 9, 6]  # Example for London
        return monthly_temps[month - 1] + random.uniform(-3, 3)
    
    def _get_time_of_day_factor(self, timestamp_ns: int) -> float:
        """Get time of day factor (0-1) for daily temperature variation."""
        seconds_in_day = (timestamp_ns // 1_000_000_000) % 86400
        hour = seconds_in_day / 3600  # 0-23.999...
        # Temperature peaks at 3 PM, lowest at 6 AM
        return math.sin((hour - 6) * math.pi / 12) * 0.5 + 0.5
    
    def _update_weather_condition(self):
        """Update weather condition based on current state and trend."""
        current_idx = WEATHER_CONDITIONS.index(self.current_condition)
        # Move condition based on trend, with some randomness
        change = self.weather_trend + random.uniform(-0.3, 0.3)
        new_idx = min(max(0, current_idx + int(change * 2)), len(WEATHER_CONDITIONS) - 1)
        self.current_condition = WEATHER_CONDITIONS[new_idx]
        
        # Slightly adjust trend over time
        self.weather_trend = max(-0.5, min(0.5, self.weather_trend + random.uniform(-0.1, 0.1)))
    
    def generate_forecast(self) -> WeatherForecast:
        """Generate a realistic weather forecast data point."""
        # Update time-based factors
        time_of_day = self._get_time_of_day_factor(self.current_time)
        
        # Temperature varies by time of day and has some randomness
        temp_variation = 8 * time_of_day  # 8°C daily variation
        temperature = self.base_temperature + temp_variation + random.uniform(-1, 1)
        
        # Update weather condition
        self._update_weather_condition()
        
        # Generate forecast
        forecast = WeatherForecast(
            timestamp=self.current_time,
            location=self.location,
            temperature=temperature,
            feels_like=temperature - (temperature * 0.1 * random.uniform(0, 1)),  # Up to 10% difference
            humidity=random.uniform(40, 90),  # %
            cloud_cover=random.uniform(0, 100),  # %
            wind_speed=random.uniform(0.5, 10),  # m/s
            wind_direction=random.uniform(0, 360),  # degrees
            pressure=random.uniform(980, 1040),  # hPa
            condition=self.current_condition,
            visibility=random.uniform(1, 20),  # km
            precipitation_prob=random.uniform(0, 100),  # %
            uv_index=min(11, max(0, time_of_day * 10 * random.uniform(0.8, 1.2)))  # 0-11
        )
        
        return forecast
    
    def run(self):
        """Generate weather forecast data every 5 minutes."""
        while self.running:
            try:
                # Generate next forecast
                forecast = self.generate_forecast()
                
                # Increment time
                self.current_time += self.time_step
                
                # Convert to dict and produce the event
                forecast_data = forecast.to_dict()
                event_serialized = self.serialize(key=self.location, value=forecast_data)
                self.produce(key=event_serialized.key, value=event_serialized.value)
                print(f"Weather forecast for {self.location} at {forecast.timestamp}")
                
                # Sleep for 5 minutes between updates
                time.sleep(300)
                
            except Exception as e:
                print(f"Error generating weather forecast: {str(e)}")
                break


def main():
    """Set up and run the weather forecast generator."""
    print(f"Starting weather forecast generator for location: {location}")
    
    # Setup necessary objects
    app = Application(consumer_group=f"weather_forecast_{location}", auto_create_topics=True)
    weather_source = WeatherForecastGenerator(name=f"weather-{location}", location=location)
    output_topic = app.topic(name=os.environ["output"])

    # Add source to application
    app.add_source(source=weather_source, topic=output_topic)
    
    # Run the application
    app.run()


# Sources require execution under a conditional main
if __name__ == "__main__":
    main()
