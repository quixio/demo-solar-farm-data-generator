import os

print("=== PASSWORD DEBUG ===")
password = os.getenv("TIMESCALEDB_PASSWORD")
print(f"Raw password value: {repr(password)}")
print(f"Password type: {type(password)}")
print(f"Password is None: {password is None}")
print(f"Password is empty: {password == ''}")
if password:
    print(f"Password length: {len(password)}")
    print(f"First 3 chars: {password[:3] if len(password) >= 3 else password}")
else:
    print("Password is falsy (None, empty, etc.)")

print("\n=== ALL ENVIRONMENT VARIABLES ===")
for key in sorted(os.environ.keys()):
    if 'TIMESCALE' in key or 'PASSWORD' in key or 'SECRET' in key:
        value = os.environ[key]
        print(f"{key}: {repr(value)}")

print("\n=== TESTING POSTGRES CONNECTION ARGS ===")
try:
    import psycopg2
    args = {
        'host': os.getenv("TIMESCALEDB_HOST", "timescaledb"),
        'port': int(os.getenv("TIMESCALEDB_PORT", "5432")),
        'dbname': os.getenv("TIMESCALEDB_DBNAME", "metrics"),
        'user': os.getenv("TIMESCALEDB_USER", "tsadmin"),
        'password': os.getenv("TIMESCALEDB_PASSWORD", "")
    }
    print("Connection args:", args)
    
    if not args['password']:
        print("ERROR: Password argument is empty!")
    else:
        print(f"Password provided: {len(args['password'])} characters")
        
except ImportError:
    print("psycopg2 not available for testing")