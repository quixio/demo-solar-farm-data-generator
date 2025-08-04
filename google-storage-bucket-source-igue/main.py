Root cause
----------
Inside test_google_storage_connection you have the line  

    import json  

nested in the elif file_format.lower() == 'json' block.  
Any import statement is treated by Python as an assignment, so the
interpreter marks json as a *local* variable for the whole function.
Because the first reference to json (the call to json.loads for the
service-account credentials) happens **before** that local variable is
initialised, Python raises

    UnboundLocalError: cannot access local variable 'json' …

Fix
---
1. Remove the inner import json (or move all imports to the top of the file).
2. Make no other reference to a local variable called json.

Minimal patch

```python
# ---- delete these two lines ----------------------------------
# elif file_format.lower() == 'json':
#     # Parse JSON content
#     import json
#     try:
# --------------------------------------------------------------

elif file_format.lower() == 'json':
    try:
        json_data = json.loads(file_content)
        ...
```

or, if you prefer to keep the comment:

```python
elif file_format.lower() == 'json':
    # Parse JSON content (json is already imported at the top)
    try:
        json_data = json.loads(file_content)
        ...
```

Why it works
------------
• Without the inner import the global-scope json module (imported at the
top of the file) is used everywhere in the function, so it is already
bound when json.loads is first called.  
• The rest of the logic is unchanged.

Additional note
---------------
After this fix the next error you may see is “Invalid JSON format in
credentials” if the value of GS_SECRET_KEY is literally the string
“GCLOUD_PK_JSON”.  Make sure the environment variable actually contains
the *contents* of the service-account JSON, or change the code to read
the file referenced by that name.