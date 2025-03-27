# utils.py
import aiosqlite # Changed import
import asyncio # Added import
import os
import logging
from datetime import datetime
import pytz
from db import get_db_connection, DB_FILE # Keep DB_FILE for potential future use, get_db_connection is now async
from logger_config import setup_logger

# Setup logger
setup_logger()
logger = logging.getLogger(__name__)

async def get_global_settings(cursor=None): # Changed to async def, cursor param might be removable later
    """Retrieve global settings from the database asynchronously."""
    conn_created = False
    if cursor is None:
        conn = await aiosqlite.connect(DB_FILE) # Use await aiosqlite.connect if no cursor
        conn.row_factory = aiosqlite.Row
        cursor = await conn.cursor()
        conn_created = True

    try:
        # Load global settings
        await cursor.execute("SELECT message, delay, message_reference_chat_id, message_reference_message_id FROM GLOBAL_SETTINGS WHERE id = 1") # Added await
        global_settings_row = await cursor.fetchone() # Added await

        if not global_settings_row:
             # Handle case where settings might not exist yet (though initialize_database should prevent this)
             logger.warning("Global settings not found in database.")
             # Return default or raise error? Returning defaults for now.
             return {
                 "message": "Default message",
                 "delay": 3600,
                 "message_reference": None
             }

        # Get column names from cursor.description
        column_names = [desc[0] for desc in cursor.description]

        # Convert the tuple to a dictionary with nested message_reference
        global_settings = dict(zip(column_names, global_settings_row))
        global_settings["message_reference"] = {
            "chat_id": global_settings.get("message_reference_chat_id"), # Use .get for safety
            "message_id": global_settings.get("message_reference_message_id") # Use .get for safety
        } if global_settings.get("message_reference_chat_id") is not None else None # Handle None case

        # Clean up redundant keys if they exist
        global_settings.pop("message_reference_chat_id", None)
        global_settings.pop("message_reference_message_id", None)

        return global_settings
    except aiosqlite.Error as e:
        logger.error(f"Error loading global settings from database: {e}")
        raise
    finally:
        if conn_created:
            await cursor.close()
            await conn.close()


async def load_data(cursor=None): # Changed to async def
    """Load all data from the database asynchronously."""
    conn_created = False
    if cursor is None:
        conn = await aiosqlite.connect(DB_FILE) # Use await aiosqlite.connect if no cursor
        conn.row_factory = aiosqlite.Row
        cursor = await conn.cursor()
        conn_created = True
    try:
        await cursor.execute("SELECT * FROM GROUPS") # Added await
        groups_rows = await cursor.fetchall() # Added await
        column_names = [desc[0] for desc in cursor.description]
        groups = {}
        for raw_row in groups_rows:
            row = dict(zip(column_names, raw_row))
            # Convert next_schedule string back to datetime object if needed, or handle potential errors
            next_schedule_dt = None
            if row.get("next_schedule"):
                try:
                    # Assuming stored as ISO format string
                    next_schedule_dt = datetime.fromisoformat(row["next_schedule"].replace('Z', '+00:00')).replace(tzinfo=pytz.UTC)
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse next_schedule '{row['next_schedule']}' for group {row['group_id']}")
                    next_schedule_dt = None # Or handle as error

            groups[row["group_id"]] = {
                "name": row["name"],
                "last_msg_id": row["last_msg_id"],
                "next_schedule": next_schedule_dt, # Store as datetime object
                "active": bool(row["active"]),
                "error_count": row["error_count"],
                "error_state": bool(row["error_state"])
            }

        # Pass the existing cursor if available
        global_settings = await get_global_settings(cursor) # Added await

        return {"global_settings": global_settings, "groups": groups}
    except aiosqlite.Error as e:
        logger.error(f"Error loading data from database: {e}")
        raise
    finally:
         if conn_created:
            await cursor.close()
            await conn.close()

# Removed unused save_data function

async def add_group(group_id, group_name, cursor=None): # Changed to async def
    """Add a new group to the database asynchronously."""
    conn_created = False
    if cursor is None:
        conn = await aiosqlite.connect(DB_FILE) # Use await aiosqlite.connect if no cursor
        cursor = await conn.cursor()
        conn_created = True
    try:
        await cursor.execute("""
            INSERT OR IGNORE INTO GROUPS
            (group_id, name, last_msg_id, next_schedule, active, error_count, error_state)
            VALUES (?, ?, NULL, NULL, 0, 0, 0)
        """, (group_id, group_name)) # Added await
        if conn_created: # Commit only if we created the connection/cursor here
             await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Error adding group: {e}")
        if conn_created: await conn.rollback() # Rollback on error if we manage the transaction
        raise
    finally:
        if conn_created:
            await cursor.close()
            await conn.close()


async def update_group_message(group_id, message_id, next_time): # Changed to async def
    """Update group's message information asynchronously with UTC datetime."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with get_db_connection() as conn: # Use async context manager
                # next_time should be a timezone-aware datetime object (UTC)
                next_schedule_iso = next_time.isoformat() if next_time else None

                await conn.execute("""
                    UPDATE GROUPS
                    SET last_msg_id = ?, next_schedule = ?
                    WHERE group_id = ?
                """, (message_id, next_schedule_iso, group_id)) # Added await
                await conn.commit() # Added await
                logger.debug(f"Updated message info for group {group_id}")
                return  # Success
        except aiosqlite.OperationalError as e: # Catch aiosqlite error
            if "database is locked" in str(e) and attempt < max_retries - 1:
                wait_time = 0.1 * (attempt + 1)
                logger.warning(f"Database locked on update_group_message, retrying in {wait_time}s... (Attempt {attempt + 1})")
                await asyncio.sleep(wait_time) # Changed to await asyncio.sleep
                continue
            logger.error(f"Error updating message info after {max_retries} attempts: {e}")
            raise
        except aiosqlite.Error as e: # Catch other aiosqlite errors
             logger.error(f"Database error updating message info: {e}")
             raise


async def update_group_status(group_id: str, active: bool): # Changed to async def
    """Update group's active status asynchronously"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with get_db_connection() as conn: # Use async context manager
                await conn.execute("""
                    UPDATE GROUPS
                    SET active = ?
                    WHERE group_id = ?
                """, (int(active), group_id)) # Added await
                await conn.commit() # Added await
                logger.info(f"Updated status for group {group_id} - Active: {active}")
                return  # Success
        except aiosqlite.OperationalError as e: # Catch aiosqlite error
            if "database is locked" in str(e) and attempt < max_retries - 1:
                wait_time = 0.1 * (attempt + 1)
                logger.warning(f"Database locked on update_group_status, retrying in {wait_time}s... (Attempt {attempt + 1})")
                await asyncio.sleep(wait_time) # Changed to await asyncio.sleep
                continue
            logger.error(f"Failed to update group status after {max_retries} attempts - {str(e)}")
            raise
        except aiosqlite.Error as e: # Catch other aiosqlite errors
             logger.error(f"Database error updating group status: {e}")
             raise


async def increment_error_count(group_id): # Changed to async def
    """Increment group's error count asynchronously."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with get_db_connection() as conn: # Use async context manager
                cursor = await conn.cursor() # Need cursor to fetch result
                await cursor.execute("""
                    UPDATE GROUPS
                    SET error_count = error_count + 1
                    WHERE group_id = ?
                """, (group_id,)) # Added await

                await cursor.execute("SELECT error_count FROM GROUPS WHERE group_id = ?", (group_id,)) # Added await
                row = await cursor.fetchone() # Added await
                count = row["error_count"] if row else 0

                await conn.commit() # Added await
                await cursor.close() # Close cursor
                return count
        except aiosqlite.OperationalError as e: # Catch aiosqlite error
            if "database is locked" in str(e) and attempt < max_retries - 1:
                wait_time = 0.1 * (attempt + 1)
                logger.warning(f"Database locked on increment_error_count, retrying in {wait_time}s... (Attempt {attempt + 1})")
                await asyncio.sleep(wait_time) # Changed to await asyncio.sleep
                continue
            logger.error(f"Error incrementing error count after {max_retries} attempts: {e}")
            raise
        except aiosqlite.Error as e: # Catch other aiosqlite errors
             logger.error(f"Database error incrementing error count: {e}")
             raise
    return 0 # Return 0 if all retries fail


async def remove_group(group_id): # Changed to async def
    """Remove a group from the database asynchronously."""
    try:
        async with get_db_connection() as conn: # Use async context manager
            await conn.execute("DELETE FROM GROUPS WHERE group_id = ?", (group_id,)) # Added await
            await conn.commit() # Added await
            logger.info(f"Removed group {group_id} from database.")
    except aiosqlite.Error as e: # Catch aiosqlite error
        logger.error(f"Error removing group {group_id}: {e}")
        raise


async def get_group(group_id: str): # Changed to async def
    """Get a specific group's data asynchronously."""
    try:
        async with get_db_connection() as conn: # Use async context manager
            cursor = await conn.cursor() # Need cursor
            await cursor.execute("SELECT * FROM GROUPS WHERE group_id = ?", (group_id,)) # Added await
            row = await cursor.fetchone() # Added await
            await cursor.close() # Close cursor

            if row:
                 # Convert next_schedule string back to datetime object if needed
                next_schedule_dt = None
                if row["next_schedule"]:
                    try:
                        next_schedule_dt = datetime.fromisoformat(row["next_schedule"].replace('Z', '+00:00')).replace(tzinfo=pytz.UTC)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse next_schedule '{row['next_schedule']}' for group {group_id} in get_group")
                        next_schedule_dt = None

                return {
                    "name": row["name"],
                    "last_msg_id": row["last_msg_id"],
                    "next_schedule": next_schedule_dt, # Return datetime object
                    "active": bool(row["active"]),
                    "error_count": row["error_count"],
                    "error_state": bool(row["error_state"])
                }
            else:
                return None # Return None if group not found

    except aiosqlite.Error as e: # Catch aiosqlite error
        logger.error(f"Error getting group data for {group_id}: {e}")
        raise # Re-raise the exception or return None/handle differently