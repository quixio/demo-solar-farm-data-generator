import os
print("Environment variables check:")
print(f"TIMESCALEDB_HOST: {os.getenv('TIMESCALEDB_HOST', 'NOT_FOUND')}")
print(f"TIMESCALEDB_PASSWORD: {os.getenv('TIMESCALEDB_PASSWORD', 'NOT_FOUND')}")
print(f"All env vars containing TIMESCALE:")
for key, value in os.environ.items():
    if 'TIMESCALE' in key:
        print(f"  {key}: {value}")