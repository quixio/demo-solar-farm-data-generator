import os
import json
import psycopg2
from datetime import datetime
from typing import Dict, Any, List
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch
from dotenv import load_dotenv

# Load environment variables from a .env file for local development
load_dotenv()

class TimescaleDBSink(BatchingSink):
    """
    Custom TimescaleDB sink for writing solar panel data.
    """
    
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str, 
                 table_name: str, on_client_connect_success=None, on_client_connect_failure=None):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure
        )
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = table_name
        self.connection = None
        self.cursor = None
    
    def setup(self):
        """
        Setup database connection and create table if it doesn't exist.
        """
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.dbname,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                timestamp TIMESTAMPTZ NOT NULL,
                panel_id VARCHAR(255),
                location_id VARCHAR(255),
                location_name VARCHAR(255),
                latitude FLOAT,
                longitude FLOAT,
                timezone INTEGER,
                power_output INTEGER,
                unit_power VARCHAR(50),
                temperature FLOAT,
                unit_temp VARCHAR(50),
                irradiance INTEGER,
                unit_irradiance VARCHAR(50),
                voltage FLOAT,
                unit_voltage VARCHAR(50),
                current INTEGER,
                unit_current VARCHAR(50),
                inverter_status VARCHAR(50)
            );
            """
            self.cursor.execute(create_table_query)
            self.connection.commit()
            
            # Create hypertable if it doesn't exist (TimescaleDB specific)
            try:
                hypertable_query = (
                    f"SELECT create_hypertable('{self.table_name}', 'timestamp', "
                    f"if_not_exists => TRUE, migrate_data => TRUE);"
                )
                self.cursor.execute(hypertable_query)
                self.connection.commit()
            except psycopg2.errors.FeatureNotSupported as e:
                # The table already contains rows – this is fine, just skip
                self.connection.rollback()
                print(f"Hypertable creation skipped: {e.pgerror.strip()}")
            except psycopg2.errors.DuplicateObject:
                # Hypertable already exists – also fine
                self.connection.rollback()
                print("Hypertable already exists, nothing to do.")
            except psycopg2.Error as e:
                self.connection.rollback()
                print(f"Hypertable creation skipped: {e.pgerror.strip()}")
            
            print(f"TimescaleDB sink setup completed for table {self.table_name}")
            
        except Exception as e:
            print(f"Failed to setup TimescaleDB connection: {e}")
            if self.on_client_connect_failure:
                self.on_client_connect_failure(e)
            raise
        
        if self.on_client_connect_success:
            self.on_client_connect_success()
    
    def write(self, batch: SinkBatch):
        """
        Write a batch of records to TimescaleDB.
        """
        if not batch:
            return
        
        insert_query = f"""
        INSERT INTO {self.table_name} (
            timestamp, panel_id, location_id, location_name, latitude, longitude,
            timezone, power_output, unit_power, temperature, unit_temp, irradiance,
            unit_irradiance, voltage, unit_voltage, current, unit_current, inverter_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """
        
        records = []
        for item in batch:
            try:
                # Parse the JSON string from the value field
                if isinstance(item.value, str):
                    data = json.loads(item.value)
                else:
                    data = item.value
                
                # Convert timestamp to datetime
                timestamp_value = data.get('timestamp', 0)
                if isinstance(timestamp_value, (int, float)):
                    # Convert from nanoseconds to seconds if necessary
                    if timestamp_value > 1e12:  # Likely nanoseconds
                        timestamp_value = timestamp_value / 1e9
                    dt = datetime.fromtimestamp(timestamp_value)
                else:
                    dt = datetime.now()
                
                record = (
                    dt,
                    data.get('panel_id'),
                    data.get('location_id'),
                    data.get('location_name'),
                    data.get('latitude'),
                    data.get('longitude'),
                    data.get('timezone'),
                    data.get('power_output'),
                    data.get('unit_power'),
                    data.get('temperature'),
                    data.get('unit_temp'),
                    data.get('irradiance'),
                    data.get('unit_irradiance'),
                    data.get('voltage'),
                    data.get('unit_voltage'),
                    data.get('current'),
                    data.get('unit_current'),
                    data.get('inverter_status')
                )
                records.append(record)
                
            except Exception as e:
                print(f"Error processing record: {e}")
                continue
        
        if records:
            try:
                self.cursor.executemany(insert_query, records)
                self.connection.commit()
                print(f"Successfully wrote {len(records)} records to TimescaleDB")
            except Exception as e:
                self.connection.rollback()
                print(f"Error writing to TimescaleDB: {e}")
                raise
    
    def flush(self):
        """
        Flush any remaining data and commit the transaction.
        """
        if self.connection:
            try:
                self.connection.commit()
            except Exception as e:
                print(f"Error during flush: {e}")
                self.connection.rollback()
                raise
    
    def close(self):
        """
        Close the database connection.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def main():
    # Get TimescaleDB connection parameters from environment variables
    host = os.environ.get('TIMESCALEDB_HOST', 'localhost')
    
    try:
        port = int(os.environ.get('TIMESCALEDB_PORT', '5432'))
    except ValueError:
        port = 5432
    
    dbname = os.environ.get('TIMESCALEDB_DBNAME', 'metrics')
    user = os.environ.get('TIMESCALEDB_USERNAME', 'tsadmin')
    password = os.environ.get('TIMESCALEDB_PASSWORD', 'password')
    table_name = os.environ.get('TIMESCALEDB_TABLENAME', 'solar_datav6')
    
    # Initialize TimescaleDB Sink
    timescale_sink = TimescaleDBSink(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        table_name=table_name
    )
    
    # Initialize the application
    app = Application(
        consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "timescaledb-sink-group"),
        auto_offset_reset="earliest",
        commit_interval=float(os.environ.get("BATCH_TIMEOUT", "1")),
        commit_every=int(os.environ.get("BATCH_SIZE", "1000"))
    )
    
    # Define the input topic
    input_topic = app.topic(os.environ.get("input", "solar-data"), key_deserializer="string")
    
    # Process and sink data
    sdf = app.dataframe(input_topic)
    
    # Debug: Print raw message structure
    sdf = sdf.apply(lambda item: print(f'Raw message: {item}') or item)
    
    # Sink data to TimescaleDB
    sdf.sink(timescale_sink)
    
    # Run the application for 10 messages
    print("Starting TimescaleDB sink application...")
    app.run(count=10, timeout=20)


if __name__ == "__main__":
    main()