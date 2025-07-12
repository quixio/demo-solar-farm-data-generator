import os
print("=== SECRET DEBUG ===")
password = os.getenv("TIMESCALEDB_PASSWORD")
print(f"TIMESCALEDB_PASSWORD from env: '{password}'")
print(f"Password is None: {password is None}")
print(f"Password is empty string: {password == ''}")
print(f"Password length: {len(password) if password else 'N/A'}")

print("\n=== ALL ENV VARS CONTAINING 'TIMESCALE' ===")
found_any = False
for key, value in os.environ.items():
    if 'TIMESCALE' in key.upper():
        print(f"{key}: '{value}'")
        found_any = True
if not found_any:
    print("No TIMESCALE variables found!")

print("\n=== CHECKING SECRET RESOLUTION ===")
# Check if we're getting the secret name instead of value
if password == "TIMESCALE_PASSWORD":
    print("ERROR: Getting secret key name instead of secret value!")
    print("This means the secret is not being resolved properly.")
elif password == "" or password is None:
    print("ERROR: Secret is empty or None!")
    print("This means the secret doesn't exist or isn't accessible.")
else:
    print(f"SUCCESS: Secret resolved to {len(password)} character password")