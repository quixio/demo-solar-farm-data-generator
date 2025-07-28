#!/bin/sh
set -e

TARGET_DIR=$PGDATA
TARGET_USER="postgres"
TARGET_GROUP="postgres"

# Function to initialize the database
initialize_db() {
    echo "Initializing PostgreSQL database in $TARGET_DIR..."
    # Ensure the directory is empty before initialization
    rm -rf "$TARGET_DIR"/* 2>/dev/null || true
    # Run initdb with the postgres user
    su -s /bin/sh -c "initdb --username=$POSTGRES_USER --pwfile=<(echo "$POSTGRES_PASSWORD") -D $TARGET_DIR" "$TARGET_USER"
    
    # Update configuration for port 80
    echo "port = 80" >> "$TARGET_DIR/postgresql.conf"
    echo "listen_addresses = '*'" >> "$TARGET_DIR/postgresql.conf"
    
    # Update pg_hba.conf to allow connections from any IP
    echo "host all all 0.0.0.0/0 md5" >> "$TARGET_DIR/pg_hba.conf"
}

# Check if directory exists and has content
if [ -d "$TARGET_DIR" ] && [ "$(ls -A $TARGET_DIR 2>/dev/null)" ]; then
    echo "Data directory $TARGET_DIR exists and is not empty"
    
    # Check if it's a valid PostgreSQL data directory
    if [ ! -f "$TARGET_DIR/PG_VERSION" ]; then
        echo "Directory exists but is not a valid PostgreSQL data directory. Reinitializing..."
        initialize_db
    else
        echo "Existing PostgreSQL data directory found. Using existing data."
    fi
else
    # Create the directory if it doesn't exist
    mkdir -p "$TARGET_DIR"
    chown -R "$TARGET_USER:$TARGET_GROUP" "$TARGET_DIR"
    initialize_db
fi

# Fix permissions
chown -R "$TARGET_USER:$TARGET_GROUP" "$TARGET_DIR"
chmod -R 0700 "$TARGET_DIR"

# Start PostgreSQL with port 80 configuration
exec su -s /bin/sh -c "docker-entrypoint.sh postgres -c listen_addresses='*' -c port=80" "$TARGET_USER"