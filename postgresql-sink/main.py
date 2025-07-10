import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Test TimescaleDB connection and basic functionality."""
    
    # Get database configuration from environment variables
    db_config = {
        'host': os.environ['TIMESCALE_HOST'],
        'port': int(os.environ.get('TIMESCALE_PORT', '5432')),
        'database': os.environ['TIMESCALE_DATABASE'], 
        'user': os.environ['TIMESCALE_USER'],
        'password': os.environ['TIMESCALE_PASSWORD']
    }
    
    logger.info(f"Attempting to connect to TimescaleDB at {db_config['host']}:{db_config['port']}")
    logger.info(f"Database: {db_config['database']}, User: {db_config['user']}")
    
    try:
        # Test database connection
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Test basic connection
                cursor.execute("SELECT version();")
                version = cursor.fetchone()
                logger.info(f"Connected successfully! PostgreSQL version: {version['version']}")
                
                # Check if TimescaleDB extension is available
                cursor.execute("SELECT * FROM pg_extension WHERE extname = 'timescaledb';")
                timescale_ext = cursor.fetchone()
                if timescale_ext:
                    logger.info("TimescaleDB extension is installed!")
                    
                    # Get TimescaleDB version
                    cursor.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';")
                    timescale_version = cursor.fetchone()
                    logger.info(f"TimescaleDB version: {timescale_version['extversion']}")
                else:
                    logger.warning("TimescaleDB extension not found - this is a regular PostgreSQL database")
                
                # Test creating a simple table
                test_table = "test_connection"
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {test_table} (
                        timestamp TIMESTAMPTZ NOT NULL,
                        sensor_id TEXT NOT NULL,
                        value DOUBLE PRECISION
                    );
                """)
                
                # If TimescaleDB is available, try to create a hypertable
                if timescale_ext:
                    try:
                        cursor.execute(f"SELECT create_hypertable('{test_table}', 'timestamp', if_not_exists => TRUE);")
                        logger.info("Successfully created/verified hypertable!")
                    except Exception as e:
                        logger.warning(f"Could not create hypertable: {e}")
                
                # Test inserting sample data
                import datetime
                now = datetime.datetime.now()
                cursor.execute(f"""
                    INSERT INTO {test_table} (timestamp, sensor_id, value) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (now, 'test_sensor', 42.0))
                
                # Test querying data
                cursor.execute(f"SELECT COUNT(*) as count FROM {test_table};")
                count = cursor.fetchone()
                logger.info(f"Test table contains {count['count']} rows")
                
                # Clean up test table
                cursor.execute(f"DROP TABLE IF EXISTS {test_table};")
                logger.info("Test completed successfully - connection verified!")
                
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise
    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

if __name__ == "__main__":
    main()