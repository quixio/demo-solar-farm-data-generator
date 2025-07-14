from quixstreams import Application
import os
import json
import psycopg2

def connect_to_timescaledb():
    connection_string = f"dbname='{os.environ.get('TIMESCALEDB_NAME')}' user='{os.environ.get('TIMESCALEDB_USER')}' " \
                        f"host='{os.environ.get('TIMESCALEDB_HOST')}' password='{os.environ.get('TIMESCALEDB_PASSWORD')}'"
    try:
        connection = psycopg2.connect(connection_string)
        return connection
    except Exception as e:
        print(f"Unable to connect to the database: {e}")
        return None

def process_message(message, conn):
    try:
        # Parse the JSON value field
        data = json.loads(message['value'])

        # Extract data
        panel_id = data['panel_id']
        location_id = data['location_id']
        location_name = data['location_name']
        latitude = data['latitude']
        longitude = data['longitude']
        timezone = data['timezone']
        power_output = data['power_output']
        temperature = data['temperature']
        irradiance = data['irradiance']
        voltage = data['voltage']
        current = data['current']
        inverter_status = data['inverter_status']
        timestamp = data['timestamp']

        # Insert data into TimescaleDB
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO solar_data (panel_id, location_id, location_name, latitude, longitude, timezone, power_output, 
        temperature, irradiance, voltage, current, inverter_status, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (panel_id, location_id, location_name, latitude, longitude, timezone, power_output, 
                                      temperature, irradiance, voltage, current, inverter_status, timestamp))
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Failed to process message: {e}")

def main():
    app = Application(
        consumer_group="solar_data_processor",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )

    input_topic = app.topic(name=os.environ["input"])

    connection = connect_to_timescaledb()

    if connection is None:
        return

    sdf = app.dataframe(topic=input_topic)
    count = 0

    for message in sdf:
        if count >= 10:
            break
        process_message(message, connection)
        count += 1

    connection.close()

    app.run(count=10)

if __name__ == "__main__":
    main()