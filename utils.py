# utils.py
import sqlite3
import time
import os
import logging
from datetime import datetime
import pytz
from db import get_db_connection, DB_FILE
from logger_config import setup_logger
def get_global_settings(cursor):
    """Retrieve global settings from the database."""
    try:
        # Load global settings
        cursor.execute("SELECT message, delay, message_reference_chat_id, message_reference_message_id FROM GLOBAL_SETTINGS WHERE id = 1")
        global_settings_row = cursor.fetchone()

        # Get column names from cursor.description
        column_names = [desc[0] for desc in cursor.description]

        # Convert the tuple to a dictionary with nested message_reference
        global_settings = dict(zip(column_names, global_settings_row))
        global_settings["message_reference"] = {
            "chat_id": global_settings["message_reference_chat_id"],
            "message_id": global_settings["message_reference_message_id"]
        }
        del global_settings["message_reference_chat_id"]
        del global_settings["message_reference_message_id"]

        return global_settings
    except sqlite3.Error as e:
        logger.error(f"Error loading global settings from database: {e}")
        raise

# Setup logger
setup_logger()
logger = logging.getLogger(__name__)

# DATA_FILE = "data.json"

# --- SQLite functions ---

def load_data(cursor=None):
    """Load all data from the database."""
    try:
        # Manage connection only if no cursor provided
        if cursor is None:
            conn = get_db_connection()
        else:
            conn = None
        cursor.execute("SELECT * FROM GROUPS")
 
        groups_rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        groups = {}
        for raw_row in groups_rows:
            row = dict(zip(column_names, raw_row))
            groups[row["group_id"]] = {
                "name": row["name"],
                "last_msg_id": row["last_msg_id"],
                "next_schedule": row["next_schedule"],
                "active": bool(row["active"]),
                "error_count": row["error_count"],
                "error_state": bool(row["error_state"])
            }

        global_settings = get_global_settings(cursor)

        if conn:  # Only close if we created the connection
            conn.close()
        
        return {"global_settings": global_settings, "groups": groups}
    except sqlite3.Error as e:
        logger.error(f"Error loading data from database: {e}")
        raise

def save_data(data):
    """Save all data to the database."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

# Update global settings
        global_settings = data["global_settings"]
        message_reference = global_settings.get("message_reference")
        cursor.execute("""
            UPDATE GLOBAL_SETTINGS
            SET message = ?, delay = ?, message_reference_chat_id = ?, message_reference_message_id = ?
            WHERE id = 1
        """, (global_settings["message"], global_settings["delay"],
              message_reference["chat_id"] if message_reference else None,  # Maintain existing formatting
              message_reference["message_id"] if message_reference else None))

        # Get existing group IDs
        cursor.execute("SELECT group_id FROM GROUPS")
        existing_group_ids = {row["group_id"] for row in cursor.fetchall()}

        # Update or insert groups
        for group_id, group_data in data["groups"].items():
            if group_id in existing_group_ids:
                cursor.execute("""
                    UPDATE GROUPS
                    SET name = ?, last_msg_id = ?, next_schedule = ?, active = ?, error_count = ?, error_state = ?
                    WHERE group_id = ?
                """, (group_data["name"], group_data["last_msg_id"], group_data["next_schedule"],
                      group_data["active"], group_data["error_count"], group_data["error_state"], group_id))
                existing_group_ids.remove(group_id)  # Remove from set to track deletions
            else:
                cursor.execute("""
                    INSERT INTO GROUPS (group_id, name, last_msg_id, next_schedule, active, error_count, error_state)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (group_id, group_data["name"], group_data["last_msg_id"], group_data["next_schedule"],
                      group_data["active"], group_data["error_count"], group_data["error_state"]))

        # Delete groups that are no longer present
        for group_id in existing_group_ids:
            cursor.execute("DELETE FROM GROUPS WHERE group_id = ?", (group_id,))

        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error saving data to database: {e}")
        raise

def add_group(group_id, group_name, cursor):
    """Add a new group to the database."""
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO GROUPS 
            (group_id, name, last_msg_id, next_schedule, active, error_count, error_state)
            VALUES (?, ?, NULL, NULL, 0, 0, 0)
        """, (group_id, group_name))
    except sqlite3.Error as e:
        logger.error(f"Error adding group: {e}")
        raise


def update_group_message(group_id, message_id, next_time):
    """Update group's message information with UTC datetime."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                # Use immediate transaction mode for better concurrency
                cursor.execute("BEGIN IMMEDIATE")
                
                cursor.execute("""
                    UPDATE GROUPS
                    SET last_msg_id = ?, next_schedule = ?
                    WHERE group_id = ?
                """, (message_id, next_time.astimezone(pytz.UTC).isoformat(), group_id))

                conn.commit()
                return  # Success
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                wait_time = 0.1 * (attempt + 1)
                logger.warning(f"Database locked, retrying in {wait_time}s... (Attempt {attempt + 1})")
                time.sleep(wait_time)
                continue
            logger.error(f"Error updating message info after {max_retries} attempts: {e}")
            raise


def update_group_status(group_id: str, active: bool):
    """Update group's active status"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                # Use immediate transaction mode for better concurrency
                cursor.execute("BEGIN IMMEDIATE")
                
                cursor.execute("""
                    UPDATE GROUPS
                    SET active = ?
                    WHERE group_id = ?
                """, (int(active), group_id))

                conn.commit()
                logger.info(f"Updated status for group {group_id} - Active: {active}")
                return  # Success
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                wait_time = 0.1 * (attempt + 1)
                logger.warning(f"Database locked, retrying in {wait_time}s... (Attempt {attempt + 1})")
                time.sleep(wait_time)
                continue
            logger.error(f"Failed to update group status after {max_retries} attempts - {str(e)}")
            raise


def increment_error_count(group_id):
    """Increment group's error count."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                # Use immediate transaction mode for better concurrency
                cursor.execute("BEGIN IMMEDIATE")
                
                cursor.execute("""
                    UPDATE GROUPS
                    SET error_count = error_count + 1
                    WHERE group_id = ?
                """, (group_id,))

                cursor.execute("SELECT error_count FROM GROUPS WHERE group_id = ?", (group_id,))
                count = cursor.fetchone()["error_count"]

                conn.commit()
                return count
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                wait_time = 0.1 * (attempt + 1)
                logger.warning(f"Database locked, retrying in {wait_time}s... (Attempt {attempt + 1})")
                time.sleep(wait_time)
                continue
            logger.error(f"Error incrementing error count after {max_retries} attempts: {e}")
            raise


def remove_group(group_id):
    """Remove a group from the database."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("DELETE FROM GROUPS WHERE group_id = ?", (group_id,))

            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error removing group: {e}")
        raise

def get_group(group_id: str):
    """Get a specific group's data."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM GROUPS WHERE group_id = ?", (group_id,))
            row = cursor.fetchone()

            return {
                "name": row["name"],
                "last_msg_id": row["last_msg_id"],
                "next_schedule": row["next_schedule"],
                "active": bool(row["active"]),
                "error_count": row["error_count"],
                "error_state": bool(row["error_state"])
            }

    except sqlite3.Error as e:
        logger.error(f"Error getting group data: {e}")
        raise