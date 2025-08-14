# Cached sandbox code for unknown
# Generated on 2025-08-14 16:17:31
# Template: Unknown
# This is cached code - delete this file to force regeneration

# DEPENDENCIES:
# pip install quixstreams
# pip install clickhouse-driver
# pip install python-dotenv
# END_DEPENDENCIES

import os
import json
import logging
from datetime import datetime
from quixstreams import Application
from clickhouse_driver import Client
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ClickHouse connection parameters
try:
    port = int(os.environ.get('CLICKHOUSE_PORT', '9000'))
except ValueError:
    port = 9000

clickhouse_client = Client(
    host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
    port=port,
    user=os.environ.get('CLICKHOUSE_USER', 'default'),
    password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
    database=os.environ.get('CLICKHOUSE_DATABASE', 'default')
)


app = Application(
    consumer_group=os.environ.get("CONSUMER_GROUP_NAME", "clickhouse-sink"),
    auto_offset_reset="earliest",
)

input_topic = app.topic(os.environ["input"])
sdf = app.dataframe(input_topic)

sdf = app.dataframe(input_topic)
sdf.sink(influxdb_v3_sink)

sdf = sdf.apply(clickhouse_sink)

if __name__ == "__main__":
    app.run()