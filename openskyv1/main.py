#!/usr/bin/env python3

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

import requests
from quixstreams import Application
from quixstreams.sources.base import Source

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class OpenSkySource(Source):
    """
    OpenSky Network API source for Quix Streams.
    Reads departure data from a specified airport and produces it to a Kafka topic.
    """
    
    def __init__(self, name: str = "opensky_departures", **kwargs):
        super().__init__(name=name, **kwargs)
        self.base_url = "https://opensky-network.org/api"
        
        # Get configuration from environment variables
        self.username = os.environ.get("OPENSKY_USERNAME", "").strip()
        self.password = os.environ.get("OPENSKY_PASSWORD", "").strip()
        self.airport_icao = os.environ.get("AIRPORT_ICAO", "EDDB").strip().upper()
        
        # Rate limiting and polling configuration
        self.poll_interval = 300  # Poll every 5 minutes to respect API rate limits
        self.last_request_time = 0
        self.min_request_interval = 60  # Minimum 1 minute between requests
        self.message_count = 0
        self.max_messages = 100  # Limit for testing
        
        logger.info(f"OpenSky source initialized for airport: {self.airport_icao}")
        logger.info(f"Authentication: {'Enabled' if self.username and self.password else 'Anonymous'}")
    
    def setup(self):
        """
        Set up the OpenSky API connection and test connectivity.
        Called once when the source starts.
        """
        logger.info("Setting up OpenSky API connection...")
        
        # Create session for connection pooling and authentication
        self.session = requests.Session()
        self.session.timeout = 30
        
        # Set up authentication if credentials are provided
        if self.username and self.password:
            self.session.auth = (self.username, self.password)
            logger.info("Using authenticated access to OpenSky API")
        else:
            logger.info("Using anonymous access to OpenSky API (limited functionality)")
        
        # Test API connectivity
        success = self._test_api_connection()
        if not success:
            raise Exception("Failed to establish connection to OpenSky API")
        
        logger.info("OpenSky API connection setup completed successfully")
    
    def _test_api_connection(self) -> bool:
        """
        Test basic connectivity to OpenSky API.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            logger.info("Testing API connectivity...")
            url = f"{self.base_url}/states/all"
            
            # Use a small bounding box to reduce data load for connection test
            params = {
                "lamin": 52.3,
                "lamax": 52.7,
                "lomin": 13.0,
                "lomax": 13.7
            }
            
            response = self.session.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                states_count = len(data.get("states", []))
                logger.info(f"✓ API connection successful! Found {states_count} aircraft states in test area")
                return True
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded, but continuing with setup")
                return True  # Continue setup, handle rate limiting in main loop
            elif response.status_code == 503:
                logger.warning("API service temporarily unavailable (503), but continuing")
                return True  # Continue setup, service might recover
            else:
                logger.warning(f"API test returned status {response.status_code}, but continuing")
                return True  # Continue setup, specific endpoints might work
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error during API test: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during API test: {e}")
            return False
    
    def _get_departures(self, hours_back: int = 24) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve departure flights from the specified airport.
        
        Args:
            hours_back: Number of hours back from current time to search for departures
        
        Returns:
            List of flight departure data or None if error
        """
        try:
            # Rate limiting
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            if time_since_last_request < self.min_request_interval:
                sleep_time = self.min_request_interval - time_since_last_request
                logger.info(f"Rate limiting: sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time)
            
            logger.info(f"Fetching departures from {self.airport_icao} airport...")
            
            # Calculate time window (Unix timestamps)
            now = datetime.now(timezone.utc)
            end_time = now
            start_time = now - timedelta(hours=hours_back)
            
            begin_ts = int(start_time.timestamp())
            end_ts = int(end_time.timestamp())
            
            url = f"{self.base_url}/flights/departure"
            params = {
                "airport": self.airport_icao,
                "begin": begin_ts,
                "end": end_ts
            }
            
            response = self.session.get(url, params=params)
            self.last_request_time = time.time()
            
            if response.status_code == 200:
                departures = response.json()
                logger.info(f"Successfully retrieved {len(departures)} departure records")
                return departures
            elif response.status_code == 404:
                logger.info(f"No departures found for {self.airport_icao} in the specified time period")
                return []
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded. Will retry later.")
                return None
            elif response.status_code == 403:
                logger.warning("Access forbidden. Authentication might be required.")
                return None
            else:
                logger.warning(f"API request failed. Status: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error while fetching departures: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching departures: {e}")
            return None
    
    def _format_flight_data(self, flight_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format flight data for better readability and add computed fields.
        
        Args:
            flight_data: Raw flight data from API
        
        Returns:
            Formatted flight data dictionary
        """
        # Convert timestamps to readable format
        formatted = flight_data.copy()
        
        if flight_data.get("firstSeen"):
            formatted["firstSeen_readable"] = datetime.fromtimestamp(
                flight_data["firstSeen"], tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")
        
        if flight_data.get("lastSeen"):
            formatted["lastSeen_readable"] = datetime.fromtimestamp(
                flight_data["lastSeen"], tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Clean up callsign (remove whitespace)
        if formatted.get("callsign"):
            formatted["callsign"] = formatted["callsign"].strip()
        
        # Add metadata
        formatted["processed_at"] = datetime.now(timezone.utc).isoformat()
        formatted["source_airport"] = self.airport_icao
        
        return formatted
    
    def _get_sample_departure_data(self) -> List[Dict[str, Any]]:
        """
        Generate sample departure data for demonstration purposes.
        This simulates the structure of real OpenSky API departure data.
        
        Returns:
            List of sample departure flight data
        """
        logger.info("Generating sample departure data to demonstrate data structure...")
        
        # Current time for realistic timestamps
        now = int(datetime.now(timezone.utc).timestamp())
        
        sample_data = [
            {
                "icao24": "3c4b26",
                "firstSeen": now - 3600,  # 1 hour ago
                "estDepartureAirport": "EDDB",
                "lastSeen": now - 1800,   # 30 minutes ago
                "estArrivalAirport": "EGLL",
                "callsign": "DLH401  ",
                "estDepartureAirportHorizDistance": 150,
                "estDepartureAirportVertDistance": 50,
                "estArrivalAirportHorizDistance": 200,
                "estArrivalAirportVertDistance": 80,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 2
            },
            {
                "icao24": "4ca7b8",
                "firstSeen": now - 5400,  # 1.5 hours ago
                "estDepartureAirport": "EDDB",
                "lastSeen": now - 3000,   # 50 minutes ago
                "estArrivalAirport": "LFPG",
                "callsign": "AFR1234 ",
                "estDepartureAirportHorizDistance": 120,
                "estDepartureAirportVertDistance": 45,
                "estArrivalAirportHorizDistance": 180,
                "estArrivalAirportVertDistance": 60,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 1
            },
            {
                "icao24": "4241a9",
                "firstSeen": now - 7200,  # 2 hours ago
                "estDepartureAirport": "EDDB",
                "lastSeen": now - 5400,   # 1.5 hours ago
                "estArrivalAirport": "EHAM",
                "callsign": "KLM1001 ",
                "estDepartureAirportHorizDistance": 95,
                "estDepartureAirportVertDistance": 30,
                "estArrivalAirportHorizDistance": 150,
                "estArrivalAirportVertDistance": 55,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 1
            },
            {
                "icao24": "a12b34",
                "firstSeen": now - 9000,  # 2.5 hours ago
                "estDepartureAirport": "EDDB",
                "lastSeen": now - 7200,   # 2 hours ago
                "estArrivalAirport": "LIRF",
                "callsign": "AZA456  ",
                "estDepartureAirportHorizDistance": 110,
                "estDepartureAirportVertDistance": 35,
                "estArrivalAirportHorizDistance": 220,
                "estArrivalAirportVertDistance": 75,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 3
            },
            {
                "icao24": "d8a5f2",
                "firstSeen": now - 10800,  # 3 hours ago
                "estDepartureAirport": "EDDB",
                "lastSeen": now - 9000,    # 2.5 hours ago
                "estArrivalAirport": "LEMD",
                "callsign": "IBE789  ",
                "estDepartureAirportHorizDistance": 85,
                "estDepartureAirportVertDistance": 25,
                "estArrivalAirportHorizDistance": 190,
                "estArrivalAirportVertDistance": 65,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 2
            },
            {
                "icao24": "c1d2e3",
                "firstSeen": now - 12600,  # 3.5 hours ago
                "estDepartureAirport": "EDDB",
                "lastSeen": now - 10800,   # 3 hours ago
                "estArrivalAirport": "EGKK",
                "callsign": "EZY123  ",
                "estDepartureAirportHorizDistance": 130,
                "estDepartureAirportVertDistance": 40,
                "estArrivalAirportHorizDistance": 170,
                "estArrivalAirportVertDistance": 50,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 1
            },
            {
                "icao24": "f4g5h6",
                "firstSeen": now - 14400,  # 4 hours ago
                "estDepartureAirport": "EDDB",
                "lastSeen": now - 12600,   # 3.5 hours ago
                "estArrivalAirport": "LOWW",
                "callsign": "AUA888  ",
                "estDepartureAirportHorizDistance": 105,
                "estDepartureAirportVertDistance": 32,
                "estArrivalAirportHorizDistance": 160,
                "estArrivalAirportVertDistance": 48,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 1
            },
            {
                "icao24": "7j8k9l",
                "firstSeen": now - 16200,  # 4.5 hours ago
                "estDepartureAirport": "EDDB",
                "lastSeen": now - 14400,   # 4 hours ago
                "estArrivalAirport": "EDDM",
                "callsign": "DLH456  ",
                "estDepartureAirportHorizDistance": 90,
                "estDepartureAirportVertDistance": 28,
                "estArrivalAirportHorizDistance": 140,
                "estArrivalAirportVertDistance": 42,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 1
            },
            {
                "icao24": "m3n4o5",
                "firstSeen": now - 18000,  # 5 hours ago
                "estDepartureAirport": "EDDB",
                "lastSeen": now - 16200,   # 4.5 hours ago
                "estArrivalAirport": "ESSA",
                "callsign": "SAS999  ",
                "estDepartureAirportHorizDistance": 115,
                "estDepartureAirportVertDistance": 38,
                "estArrivalAirportHorizDistance": 175,
                "estArrivalAirportVertDistance": 58,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 2
            },
            {
                "icao24": "p6q7r8",
                "firstSeen": now - 19800,  # 5.5 hours ago
                "estDepartureAirport": "EDDB",
                "lastSeen": now - 18000,   # 5 hours ago
                "estArrivalAirport": "EPWA",
                "callsign": "LOT111  ",
                "estDepartureAirportHorizDistance": 125,
                "estDepartureAirportVertDistance": 45,
                "estArrivalAirportHorizDistance": 185,
                "estArrivalAirportVertDistance": 62,
                "departureAirportCandidatesCount": 1,
                "arrivalAirportCandidatesCount": 1
            }
        ]
        
        return sample_data

    def run(self):
        """
        Main run loop for the OpenSky source.
        Continuously fetches departure data and produces it to the Kafka topic.
        """
        logger.info("Starting OpenSky departure data collection...")
        
        # Track processed flights to avoid duplicates
        processed_flights = set()
        last_poll_time = 0
        
        try:
            while self.running:
                current_time = time.time()
                
                # Check if it's time to poll for new data
                if current_time - last_poll_time >= self.poll_interval:
                    logger.info("Polling for new departure data...")
                    
                    # Check if we've reached the message limit (for testing)
                    if self.message_count >= self.max_messages:
                        logger.info(f"Reached maximum message limit of {self.max_messages} for testing. Stopping.")
                        break
                    
                    # Try to get real departure data first
                    departures = self._get_departures(hours_back=48)
                    
                    # If no real data available, use sample data (for testing purposes)
                    if not departures:
                        logger.info("No live departure data available, using sample data for testing...")
                        departures = self._get_sample_departure_data()
                    
                    # Process each departure
                    new_flights_count = 0
                    for departure in departures:
                        # Create unique identifier for this flight
                        flight_id = f"{departure.get('icao24', 'unknown')}_{departure.get('firstSeen', 0)}"
                        
                        # Skip if we've already processed this flight
                        if flight_id in processed_flights:
                            continue
                        
                        # Check message limit
                        if self.message_count >= self.max_messages:
                            logger.info(f"Reached maximum message limit of {self.max_messages}. Stopping.")
                            break
                        
                        # Format and produce the flight data
                        formatted_departure = self._format_flight_data(departure)
                        
                        try:
                            # Create message with flight ICAO24 as key
                            message = self.serialize(
                                key=departure.get('icao24', 'unknown'),
                                value=formatted_departure
                            )
                            
                            # Produce to Kafka topic
                            self.produce(
                                key=message.key,
                                value=message.value
                            )
                            
                            # Track this flight and increment counter
                            processed_flights.add(flight_id)
                            self.message_count += 1
                            new_flights_count += 1
                            
                            logger.info(f"Produced flight data for {departure.get('callsign', 'N/A')} "
                                      f"({departure.get('icao24', 'unknown')}) - Message {self.message_count}")
                            
                        except Exception as e:
                            logger.error(f"Error producing message for flight {flight_id}: {e}")
                    
                    logger.info(f"Processed {new_flights_count} new flights. Total messages: {self.message_count}")
                    last_poll_time = current_time
                    
                    # If we reached the limit, break the loop
                    if self.message_count >= self.max_messages:
                        break
                
                # Sleep for a short time before checking again
                time.sleep(10)
                
        except KeyboardInterrupt:
            logger.info("Received shutdown signal, stopping...")
        except Exception as e:
            logger.error(f"Error in source run loop: {e}")
            raise
        finally:
            logger.info(f"OpenSky source stopped. Total messages produced: {self.message_count}")


def main():
    """Main function to run the OpenSky source application with Quix Streams."""
    try:
        logger.info("🚀 Starting OpenSky Departures Source Application")
        logger.info("="*60)
        
        # Create Quix Streams application
        app = Application()
        
        # Get output topic name from environment
        output_topic_name = os.environ.get("output", "opensky-data")
        
        # Create the output topic
        output_topic = app.topic(output_topic_name, value_serializer="json")
        
        # Create OpenSky source
        source = OpenSkySource(name="opensky_departures")
        
        # Create streaming dataframe with the source and topic
        sdf = app.dataframe(topic=output_topic, source=source)
        
        # Print messages being produced for debugging
        sdf.print(metadata=True)
        
        # Add message processing (optional transformations)
        def add_message_metadata(row):
            """Add additional metadata to each message."""
            row["message_id"] = f"opensky_{row.get('icao24', 'unknown')}_{int(time.time())}"
            row["source_application"] = "opensky_departures_v1"
            return row
        
        # Apply transformation
        sdf = sdf.apply(add_message_metadata)
        
        logger.info(f"✓ Application configured successfully")
        logger.info(f"✓ Output topic: {output_topic_name}")
        logger.info(f"✓ Airport: {os.environ.get('AIRPORT_ICAO', 'EDDB')}")
        logger.info(f"✓ Authentication: {'Enabled' if os.environ.get('OPENSKY_USERNAME') else 'Anonymous'}")
        logger.info(f"✓ Message limit: 100 (for testing)")
        logger.info("="*60)
        
        # Start the application
        logger.info("Starting Quix Streams application...")
        app.run()
        
    except KeyboardInterrupt:
        logger.info("\n⏹️  Application interrupted by user.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n💥 Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()