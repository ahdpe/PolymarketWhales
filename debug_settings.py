
import json
import os

SETTINGS_FILE = 'user_settings.json'

if os.path.exists(SETTINGS_FILE):
    with open(SETTINGS_FILE, 'r') as f:
        data = json.load(f)
        print(json.dumps(data.get('filters', {}), indent=2))
else:
    print("Settings file not found.")
