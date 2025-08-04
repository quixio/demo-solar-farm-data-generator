Root cause  
Python decided that the identifier json is a local variable for the whole function because later in the function you do a second, inner  import json.  
• When execution first reaches json.loads(credentials_json) the inner import has not run yet, so the local variable json is still un-initialised → UnboundLocalError.  
• The global json module you imported at the top of the file is hidden by that still-empty local variable.

Because the exception is raised before you even try to use the service-account key, the connection is never attempted.

Fix  

1. Delete (or rename) the inner import json that lives in the JSON-reading branch.  
2. Keep using the json module that was already imported at the top of the file.

Minimal patch (only the relevant lines shown):

```diff
@@
     elif file_format.lower() == 'json':
-        # Read JSON data
-        try:
-            import json                      #  <-- REMOVE THIS LINE
-            data = json.loads(file_content)
+        # Read JSON data
+        try:
+            data = json.loads(file_content)
```

Alternative if you really want a local import:

```python
import json as _json   # at top of file
...
data = _json.loads(file_content)
```

After this change the function will run until it actually tries to decode the service-account key. If GS_SECRET_KEY still contains the literal string “GCLOUD_PK_JSON” (as shown in your environment variables) you will immediately get a “Invalid JSON format in credentials” error. Put the real service-account JSON (or the path to the JSON file and adjust the code to read the file) in GS_SECRET_KEY.

Why this works  

• Removing the second import eliminates the accidental local shadowing, so json inside json.loads(...) now refers to the globally imported json module.  
• The exception handler except json.JSONDecodeError will also work, because json is now the proper module object.  
• Once the shadowing bug is fixed, the code proceeds to authenticate with Google Storage; any further errors will be genuine authentication or network problems instead of a Python scoping issue.

Apply the patch, redeploy, and rerun the connection test.