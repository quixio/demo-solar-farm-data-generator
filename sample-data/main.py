# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
# For sources, see https://quix.io/docs/quix-streams/connectors/sources/index.html
from quixstreams import Application
from quixstreams.sources import Source
import time
import os
import random
import math
from dataclasses import dataclass, field
from typing import List, Dict, Tuple
import uuid

location = os.environ["location"] # e.g. LONDON

@dataclass
class Location:
    """Represents a physical location for solar panels."""
    location_id: str
    name: str
    latitude: float
    longitude: float
    timezone: int  # UTC offset in hours
    peak_irradiance: float  # W/m² at peak sun
    
    def __post_init__(self):
        # Ensure location_id is uppercase
        self.location_id = self.location_id.upper()


@dataclass
class SolarPanel:
    """Represents a single solar panel with unique characteristics."""
    panel_id: str
    location: Location
    base_power: float = 250.0  # Base power output in W
    base_irradiance: float = 800.0  # Base irradiance in W/m²
    base_voltage: float = 24.0  # Base voltage in V
    efficiency: float = 1.0  # Panel efficiency (0.8-1.2)
    degradation_rate: float = 0.0  # Annual degradation rate
    
    def __post_init__(self):
        # Add some variation to panel characteristics
        self.efficiency = max(0.8, min(1.2, random.normalvariate(1.0, 0.05)))
        self.degradation_rate = random.uniform(0.005, 0.02)  # 0.5% to 2% annual degradation
        self.base_power *= self.efficiency
        self.base_irradiance *= self.efficiency


class SolarDataGenerator(Source):
    """
    A Quix Streams Source that generates realistic solar panel data over time.
    Simulates power output, temperature, irradiance, voltage, current, and inverter status
    for multiple solar panels.
    """
    
    def __init__(self, name: str, num_panels: int = 100):
        Source.__init__(self, name)
        
        # Define all possible locations
        all_locations = [
            Location("LONDON", "London, UK", 51.5074, -0.1278, 1, 850.0),
            Location("MADRID", "Madrid, Spain", 40.4168, -3.7038, 2, 950.0),
            Location("BERLIN", "Berlin, Germany", 52.5200, 13.4050, 2, 900.0),
            Location("ROME", "Rome, Italy", 41.9028, 12.4964, 2, 920.0),
            Location("PARIS", "Paris, France", 48.8566, 2.3522, 2, 870.0),
            Location("AMSTERDAM", "Amsterdam, Netherlands", 52.3676, 4.9041, 2, 830.0),
            Location("VIENNA", "Vienna, Austria", 48.2082, 16.3738, 2, 880.0),
            Location("DUBLIN", "Dublin, Ireland", 53.3498, -6.2603, 1, 800.0),
            Location("PRAGUE", "Prague, Czech Republic", 50.0755, 14.4378, 2, 860.0),
            Location("ATHENS", "Athens, Greece", 37.9838, 23.7275, 3, 980.0)
        ]
        
        # Find the specified location (case-insensitive)
        location_upper = location.upper()
        selected_location = next((loc for loc in all_locations if loc.location_id.upper() == location_upper), None)
        
        if not selected_location:
            valid_locations = ", ".join([f'"{loc.location_id}"' for loc in all_locations])
            raise ValueError(f"Invalid location: '{location}'. Valid locations are: {valid_locations}")
        
        print(f"Generating data for location: {selected_location.name} ({selected_location.location_id})")
        
        # Initialize panels for the selected location only
        self.panels = []
        for i in range(1, num_panels + 1):
            panel_id = f"{selected_location.location_id}-P{str(i).zfill(4)}"  # 4-digit panel number
            self.panels.append(SolarPanel(
                panel_id=panel_id,
                location=selected_location,
                base_irradiance=selected_location.peak_irradiance * random.uniform(0.95, 1.05)  # Slight variation per panel
            ))
        
        # Base values that are common to all panels
        self.base_temp = 25.0  # Base temperature in C
        
        # Time step in nanoseconds (1 second)
        self.time_step = 1000000000
        
        # Initialize time to current time in nanoseconds since epoch
        self.current_time = int(time.time() * 1_000_000_000)  # Current time in nanoseconds
        
        # Track panel ages in seconds
        self.panel_ages = {panel.panel_id: 0 for panel in self.panels}
        
    def _get_solar_intensity(self, hour: float) -> float:
        """Calculate solar intensity using a smooth bell curve.
        
        Args:
            hour: Current hour (0-24)
            
        Returns:
            Normalized solar intensity (0.0 to 1.0)
        """
        # Solar noon is at 12:00, with full sun between 8am and 4pm
        solar_noon = 12.0
        
        # Calculate time from solar noon in hours
        delta = abs(hour - solar_noon)
        
        # Use a Gaussian function for the bell curve
        # Standard deviation controls the width of the curve
        sigma = 4.0  # Higher values make the curve wider
        
        # Calculate intensity using Gaussian function
        intensity = math.exp(-0.5 * (delta / sigma) ** 2)
        
        # Ensure intensity is 0 at night
        if hour < 5 or hour > 19:  # Night time
            return 0.0
            
        # Apply a small ramp up/down at sunrise/sunset
        if 5 <= hour < 6:  # Sunrise ramp up
            return intensity * ((hour - 5) / 1.0)
        elif 18 <= hour <= 19:  # Sunset ramp down
            return intensity * ((19 - hour) / 1.0)
            
        return intensity

    def generate_panel_data(self, panel: SolarPanel, current_time: int) -> dict:
        """Generate data for a single solar panel."""
        # Get current hour with fractional part for smooth transitions
        seconds_in_day = (current_time // 1000000000) % 86400
        hour = seconds_in_day / 3600.0  # Convert to fractional hours
        
        # Calculate solar intensity (0.0 to 1.0)
        solar_intensity = self._get_solar_intensity(hour)
        
        # Temperature varies with solar intensity and time of day
        # Daytime heating
        # Extra warming in afternoon
        temperature = self.base_temp + \
                     (solar_intensity * 15) + \
                     (0.5 * (hour - 12) / 6)     
        
        # Calculate degradation factor (very small per-second degradation)
        degradation = 1.0 - (self.panel_ages[panel.panel_id] * panel.degradation_rate / (365 * 24 * 3600))
        
        # Power output follows solar intensity with temperature derating and degradation
        power_output = panel.base_power * solar_intensity * degradation * \
                      (1 - 0.004 * (temperature - 25))  # Temperature effect
        
        # Irradiance is directly related to solar intensity with panel efficiency
        irradiance = panel.base_irradiance * solar_intensity * panel.efficiency
        
        # Voltage decreases slightly with temperature and has panel-specific variations
        voltage = panel.base_voltage * (1 - 0.002 * (temperature - 25)) * \
                 random.uniform(0.98, 1.02)  # Panel-specific variation
        
        # Current is derived from power and voltage
        current = (power_output / voltage) if voltage > 0 else 0
        
        # Add realistic random variations
        power_output = max(0, power_output * random.uniform(0.98, 1.02))  # ±2% variation
        temperature += random.uniform(-0.5, 0.5)  # Small temperature fluctuations
        irradiance = max(0, irradiance * random.uniform(0.97, 1.03))  # ±3% variation
        voltage = max(0, voltage * random.uniform(0.998, 1.002))  # Very small voltage variation
        current = max(0, current * random.uniform(0.99, 1.01))  # Small current variation
        
        return {
            "panel_id": panel.panel_id,
            "location_id": panel.location.location_id,
            "location_name": panel.location.name,
            "latitude": panel.location.latitude,
            "longitude": panel.location.longitude,
            "timezone": panel.location.timezone,
            "power_output": round(power_output, 1),
            "unit_power": "W",
            "temperature": round(temperature, 1),
            "unit_temp": "C",
            "irradiance": round(irradiance, 1),
            "unit_irradiance": "W/m²",
            "voltage": round(voltage, 1),
            "unit_voltage": "V",
            "current": round(current, 1),
            "unit_current": "A",
            "inverter_status": "OK" if power_output > 0 else "STANDBY",
            "timestamp": self.current_time
        }
    
    def run(self):
        """Generate data points for all panels every second"""
        while self.running:
            try:
                # Update panel ages
                for panel_id in self.panel_ages:
                    self.panel_ages[panel_id] += 1  # Increment age by 1 second
                
                # Generate data for all panels
                for panel in self.panels:
                    # Generate data for this panel
                    event = self.generate_panel_data(panel, self.current_time)
                    
                    # Add timestamp
                    event["timestamp"] = self.current_time
                    
                    # Serialize and produce the event with location_id as key
                    event_serialized = self.serialize(key=event["location_id"], value=event)
                    self.produce(key=event_serialized.key, value=event_serialized.value)
                
                if self.current_time % 10 == 0:  # Print every 10 seconds to reduce noise
                    print(f"Produced data for {len(self.panels)} panels at time {self.current_time}")
                
                # Increment time
                self.current_time += self.time_step
                
                # Sleep for 1 second between batches
                time.sleep(1)
                
            except Exception as e:
                print(f"Error generating data: {str(e)}")
                break



def main():
    """ Here we will set up our Application. """

    # Setup necessary objects
    app = Application(consumer_group="data_producer", auto_create_topics=True)
    # memory_usage_source = MemoryUsageGenerator(name="memory-usage-producer")
    solar = SolarDataGenerator(name="solar-data-generator")

    output_topic = app.topic(name=os.environ["output"])

    # --- Setup Source ---
    # OPTION 1: no additional processing with a StreamingDataFrame
    # Generally the recommended approach; no additional operations needed!
    app.add_source(source=solar, topic=output_topic)

    # OPTION 2: additional processing with a StreamingDataFrame
    # Useful for consolidating additional data cleanup into 1 Application.
    # In this case, do NOT use `app.add_source()`.
    # sdf = app.dataframe(source=source)
    # <sdf operations here>
    # sdf.to_topic(topic=output_topic) # you must do this to output your data!

    # With our pipeline defined, now run the Application
    app.run()


#  Sources require execution under a conditional main
if __name__ == "__main__":
    main()
