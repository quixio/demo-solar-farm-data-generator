import os
print("=== ENVIRONMENT VARIABLE DEBUG ===")
print(f"TIMESCALEDB_PASSWORD value: '{os.getenv('TIMESCALEDB_PASSWORD', 'MISSING')}'")
print(f"TIMESCALEDB_PASSWORD length: {len(os.getenv('TIMESCALEDB_PASSWORD', ''))}")
print(f"TIMESCALEDB_HOST: '{os.getenv('TIMESCALEDB_HOST', 'MISSING')}'") 
print(f"TIMESCALEDB_USER: '{os.getenv('TIMESCALEDB_USER', 'MISSING')}'")

print("\n=== ALL TIMESCALE VARS ===")
for key, value in os.environ.items():
    if 'TIMESCALE' in key.upper():
        print(f"{key}: '{value}'")

print("\n=== TESTING CONNECTION PARAMS ===")
host = os.getenv("TIMESCALEDB_HOST", "timescaledb")
port = int(os.getenv("TIMESCALEDB_PORT", "5432"))
dbname = os.getenv("TIMESCALEDB_DBNAME", "metrics")
user = os.getenv("TIMESCALEDB_USER", "tsadmin")
password = os.getenv("TIMESCALEDB_PASSWORD", "")

print(f"host='{host}', port={port}, dbname='{dbname}', user='{user}', password='{password}' (len: {len(password)})")

# Test if password is empty
if not password:
    print("ERROR: Password is empty or missing!")
else:
    print(f"Password found: {len(password)} characters")