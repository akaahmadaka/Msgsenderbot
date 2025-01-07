# utils.py
import json
import os

DATA_FILE = "data.json"

def load_data():
    """Load group data from the JSON file."""
    if not os.path.exists(DATA_FILE):
        return {
            "global_settings": {
                "message": "Default message",
                "delay": 3600
            },
            "groups": {}
        }
    with open(DATA_FILE, "r") as file:
        return json.load(file)

def save_data(data):
    """Save group data to the JSON file."""
    with open(DATA_FILE, "w") as file:
        json.dump(data, file, indent=4)

def add_group(group_id, group_name):
    """Add a new group to the data file."""
    data = load_data()
    if group_id not in data["groups"]:
        data["groups"][group_id] = {
            "name": group_name,
            "loop_running": False,
            "last_message_id": None,
            "next_run_time": None
        }
        save_data(data)
    return data["groups"][group_id]

def remove_group(group_id):
    """Remove a group from the data file."""
    data = load_data()
    if group_id in data["groups"]:
        del data["groups"][group_id]
        save_data(data)

def get_global_settings():
    """Get global message and delay settings."""
    data = load_data()
    return data.get("global_settings", {
        "message": "Default message",
        "delay": 3600
    })

def update_global_settings(message=None, delay=None):
    """Update global message and/or delay settings."""
    data = load_data()
    if "global_settings" not in data:
        data["global_settings"] = {
            "message": "Default message",
            "delay": 3600
        }
    
    if message is not None:
        data["global_settings"]["message"] = message
    if delay is not None:
        data["global_settings"]["delay"] = delay
    
    save_data(data)
    return data["global_settings"]