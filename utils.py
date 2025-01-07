# utils.py
import json
import os
import logging
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
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
                    "delay": 3600
                },
                "groups": {}
            }
            save_data(default_data)
            return default_data
            
        with open(DATA_FILE, "r") as file:
            data = json.load(file)
            
            # Ensure all required fields exist
            if "global_settings" not in data:
                data["global_settings"] = {
                    "message": "Default message",
                    "delay": 3600
                }
            if "groups" not in data:
                data["groups"] = {}
                
            return data
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return {
            "global_settings": {
                "message": "Default message",
                "delay": 3600
            },
            "groups": {}
        }

def save_data(data):
    """Save group data to the JSON file."""
    try:
        with open(DATA_FILE, "w") as file:
            json.dump(data, file, indent=4)
        logger.debug("Data saved successfully")
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        raise

def add_group(group_id, group_name):
    """Add a new group to the data file."""
    try:
        data = load_data()
        current_time = datetime.now(timezone.utc).isoformat()
        
        if group_id not in data["groups"]:
            data["groups"][group_id] = {
                "name": group_name,
                "loop_running": False,
                "last_message_id": None,
                "next_run_time": None,
                "retry_count": 0,
                "last_error_time": None,
                "added_time": current_time,
                "last_updated": current_time
            }
        else:
            # Update existing group's name and last_updated
            data["groups"][group_id].update({
                "name": group_name,
                "last_updated": current_time
            })
            
        save_data(data)
        logger.info(f"Group {group_id} ({group_name}) added/updated successfully")
        return data["groups"][group_id]
        
    except Exception as e:
        logger.error(f"Error adding/updating group {group_id}: {e}")
        raise

def remove_group(group_id):
    """Remove a group from the data file."""
    try:
        data = load_data()
        if group_id in data["groups"]:
            group_name = data["groups"][group_id].get("name", "Unknown")
            del data["groups"][group_id]
            save_data(data)
            logger.info(f"Group {group_id} ({group_name}) removed successfully")
            return True
        return False
    except Exception as e:
        logger.error(f"Error removing group {group_id}: {e}")
        raise

def get_global_settings():
    """Get global message and delay settings."""
    try:
        data = load_data()
        settings = data.get("global_settings", {
            "message": "Default message",
            "delay": 3600
        })
        return settings
    except Exception as e:
        logger.error(f"Error getting global settings: {e}")
        return {
            "message": "Default message",
            "delay": 3600
        }

def update_global_settings(message=None, delay=None):
    """Update global message and/or delay settings."""
    try:
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
        
        # Add timestamp for when settings were last updated
        data["global_settings"]["last_updated"] = datetime.now(timezone.utc).isoformat()
        
        save_data(data)
        logger.info("Global settings updated successfully")
        return data["global_settings"]
    except Exception as e:
        logger.error(f"Error updating global settings: {e}")
        raise

def update_group_status(group_id, status_update):
    """Update specific group status fields."""
    try:
        data = load_data()
        if group_id in data["groups"]:
            data["groups"][group_id].update(status_update)
            data["groups"][group_id]["last_updated"] = datetime.now(timezone.utc).isoformat()
            save_data(data)
            logger.debug(f"Group {group_id} status updated: {status_update}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error updating group {group_id} status: {e}")
        raise

def increment_retry_count(group_id):
    """Increment the retry count for a group and return the new count."""
    try:
        data = load_data()
        if group_id in data["groups"]:
            current_count = data["groups"][group_id].get("retry_count", 0)
            data["groups"][group_id]["retry_count"] = current_count + 1
            data["groups"][group_id]["last_error_time"] = datetime.now(timezone.utc).isoformat()
            save_data(data)
            logger.info(f"Group {group_id} retry count incremented to {current_count + 1}")
            return current_count + 1
        return 0
    except Exception as e:
        logger.error(f"Error incrementing retry count for group {group_id}: {e}")
        return 0

def reset_retry_count(group_id):
    """Reset the retry count for a group."""
    try:
        data = load_data()
        if group_id in data["groups"]:
            data["groups"][group_id]["retry_count"] = 0
            data["groups"][group_id]["last_error_time"] = None
            save_data(data)
            logger.info(f"Group {group_id} retry count reset")
            return True
        return False
    except Exception as e:
        logger.error(f"Error resetting retry count for group {group_id}: {e}")
        return False
