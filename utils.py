# utils.py
import json
import os
import logging
from datetime import datetime
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATA_FILE = "data.json"

def load_data():
    """Load group data from the JSON file."""
    try:
        if not os.path.exists(DATA_FILE):
            default_data = {
                "global_settings": {
                    "message": "Default message",
                    "delay": 3600,
                    "message_reference": None  # Added message_reference
                },
                "groups": {}
            }
            save_data(default_data)
            return default_data
            
        with open(DATA_FILE, "r") as file:
            return json.load(file)
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return {
            "global_settings": {
                "message": "Default message",
                "delay": 3600,
                "message_reference": None  # Added message_reference
            },
            "groups": {}
        }

def save_data(data):
    """Save group data to the JSON file."""
    try:
        with open(DATA_FILE, "w") as file:
            json.dump(data, file, indent=4)
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        raise

def add_group(group_id, group_name):
    """Add a new group to the data file."""
    try:
        data = load_data()
        if group_id not in data["groups"]:
            data["groups"][group_id] = {
                "name": group_name,
                "last_msg_id": None,
                "next_schedule": None,  # Will be set when loop starts
                "active": False,
                "error_count": 0,
                "error_state": False
            }
            save_data(data)
        return data["groups"][group_id]
    except Exception as e:
        logger.error(f"Error adding group: {e}")
        raise

def update_group_message(group_id, message_id, next_time):
    """Update group's message information with UTC datetime."""
    try:
        data = load_data()
        if group_id in data["groups"]:
            data["groups"][group_id]["last_msg_id"] = message_id
            # Store as ISO format UTC datetime
            data["groups"][group_id]["next_schedule"] = next_time.astimezone(pytz.UTC).isoformat()
            save_data(data)
            logger.info(f"Updated message info for group {group_id} - Next: {next_time.isoformat()}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error updating message info: {e}")
        return False

def update_group_status(group_id: str, active: bool):
    """Update group's active status"""
    try:
        data = load_data()
        if group_id in data["groups"]:
            data["groups"][group_id]["active"] = active
            save_data(data)
            logger.info(f"Updated status for group {group_id} - Active: {active}")
    except Exception as e:
        logger.error(f"Failed to update group status - {str(e)}")

def increment_error_count(group_id):
    """Increment group's error count."""
    try:
        data = load_data()
        if group_id in data["groups"]:
            data["groups"][group_id]["error_count"] += 1
            save_data(data)
            return data["groups"][group_id]["error_count"]
        return 0
    except Exception as e:
        logger.error(f"Error incrementing error count: {e}")
        return 0

def remove_group(group_id):
    """Remove a group from the data file."""
    try:
        data = load_data()
        if group_id in data["groups"]:
            del data["groups"][group_id]
            save_data(data)
            return True
        return False
    except Exception as e:
        logger.error(f"Error removing group: {e}")
        return False

def get_global_settings():
    """Get global message and delay settings."""
    data = load_data()
    return data.get("global_settings", {
        "message": "Default message",
        "delay": 3600,
        "message_reference": None  # Added message_reference
    })

def update_global_settings(message=None, delay=None, message_reference=None):
    """Update global message and/or delay settings."""
    try:
        data = load_data()
        if message is not None:
            data["global_settings"]["message"] = message
        if delay is not None:
            data["global_settings"]["delay"] = delay
        if message_reference is not None:  # Added message_reference handling
            data["global_settings"]["message_reference"] = message_reference
        save_data(data)
        return data["global_settings"]
    except Exception as e:
        logger.error(f"Error updating global settings: {e}")
        raise