import asyncpg # Replaced aiosqlite
import asyncio
import logging
from datetime import datetime
import pytz # Keep for potential timezone handling if needed, though asyncpg might handle TIMESTAMPTZ well
# Removed functools wraps
from db import get_db_connection # Keep this
from logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

# Removed MAX_RETRIES, RETRY_BASE_DELAY, and with_db_retry decorator

async def get_global_settings():
    """
    Retrieve global settings (currently just delay) from the database.

    Returns:
        dict: A dictionary containing global settings (e.g., {'delay': 3600}).
              Returns default delay if not found or on error.
    Raises:
        asyncpg.PostgresError: If there's an unrecoverable error with database operations.
    """
    try:
        async with get_db_connection() as conn:
            # Use fetchrow for a single expected row
            query = "SELECT delay FROM GLOBAL_SETTINGS WHERE id = 1"
            global_settings_row = await conn.fetchrow(query)

            if not global_settings_row or global_settings_row["delay"] is None:
                logger.warning("Global delay not found in database, using default.")
                from config import GLOBAL_DELAY
                return {"delay": GLOBAL_DELAY}

            return {"delay": global_settings_row["delay"]}
    except asyncpg.PostgresError as e:
        logger.error(f"Error loading global settings from database: {e}")
        # Decide on fallback behavior: raise or return default? Returning default might be safer.
        logger.warning("Returning default global delay due to database error.")
        from config import GLOBAL_DELAY
        return {"delay": GLOBAL_DELAY}
    except Exception as e:
        logger.error(f"Unexpected error loading global settings: {e}", exc_info=True)
        logger.warning("Returning default global delay due to unexpected error.")
        from config import GLOBAL_DELAY
        return {"delay": GLOBAL_DELAY}


async def load_data():
    """
    Load all data from the database including groups and global settings.

    Returns:
        dict: A dictionary containing global_settings and groups.
    Raises:
        asyncpg.PostgresError: If there's an error with database operations.
    """
    try:
        groups = {}
        async with get_db_connection() as conn:
            # Use fetch for multiple rows
            query = """
            SELECT group_id, name, last_msg_id, next_schedule, active, retry_count, current_message_index, created_at, updated_at
            FROM GROUPS
            """
            groups_rows = await conn.fetch(query)

            for row in groups_rows:
                # asyncpg returns datetime objects for TIMESTAMPTZ, usually timezone-aware
                next_schedule_dt = row["next_schedule"] # Directly use the datetime object
                created_at_dt = row["created_at"]
                updated_at_dt = row["updated_at"]

                groups[row["group_id"]] = {
                    "name": row["name"],
                    "last_msg_id": row["last_msg_id"],
                    "next_schedule": next_schedule_dt, # Already datetime
                    "active": bool(row["active"]), # Ensure boolean
                    "retry_count": row["retry_count"],
                    "current_message_index": row["current_message_index"],
                    "created_at": created_at_dt, # Already datetime
                    "updated_at": updated_at_dt  # Already datetime
                }

        global_settings = await get_global_settings()
        return {"global_settings": global_settings, "groups": groups}
    except asyncpg.PostgresError as e:
        logger.error(f"Error loading data from database: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading data: {e}", exc_info=True)
        raise


async def add_group(group_id: str, group_name: str):
    """
    Add a new group to the database or update its name if it exists.
    Uses PostgreSQL's ON CONFLICT clause for UPSERT behavior.

    Args:
        group_id (str): The unique identifier for the group.
        group_name (str): The name of the group.
    Raises:
        asyncpg.PostgresError: If there's an error with database operations.
    """
    try:
        async with get_db_connection() as conn:
            # Use INSERT ... ON CONFLICT for UPSERT
            query = """
            INSERT INTO GROUPS (group_id, name, created_at, updated_at)
            VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (group_id) DO UPDATE SET
                name = EXCLUDED.name,
                updated_at = CURRENT_TIMESTAMP
            """
            # Use execute for commands that don't return rows
            await conn.execute(query, group_id, group_name)
            logger.debug(f"Group {group_id} ({group_name}) added or updated in database")
            # No explicit commit needed with asyncpg execute usually,
            # unless inside an explicit transaction block for multiple operations.
            return
    except asyncpg.PostgresError as e:
        logger.error(f"Error adding/updating group {group_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error adding/updating group {group_id}: {e}", exc_info=True)
        raise


async def update_group_after_send(group_id: str, message_id: int, next_message_index: int, next_time: datetime):
    """
    Update a group's state after successfully sending a message.

    Args:
        group_id (str): The unique identifier for the group.
        message_id (int): The ID of the message just sent.
        next_message_index (int): The index of the *next* message to be sent.
        next_time (datetime): The next scheduled time (should be timezone-aware).
    Raises:
        asyncpg.PostgresError: If there's an error with database operations.
    """
    try:
        async with get_db_connection() as conn:
            # Ensure next_time is timezone-aware if it isn't already
            if next_time and next_time.tzinfo is None:
                 logger.warning(f"update_group_after_send received naive datetime for group {group_id}. Assuming UTC.")
                 next_time = pytz.utc.localize(next_time)

            query = """
            UPDATE GROUPS
            SET last_msg_id = $1, next_schedule = $2, current_message_index = $3, updated_at = CURRENT_TIMESTAMP
            WHERE group_id = $4
            """
            # Pass parameters in order corresponding to $1, $2, ...
            await conn.execute(query, message_id, next_time, next_message_index, group_id)
            logger.debug(f"Updated group {group_id} after send: last_msg={message_id}, next_idx={next_message_index}, next_schedule={next_time.isoformat() if next_time else 'None'}")
            return True
    except asyncpg.PostgresError as e:
        logger.error(f"Error updating group {group_id} after send: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating group {group_id} after send: {e}", exc_info=True)
        raise


async def update_group_status(group_id: str, active: bool):
    """
    Update a group's active status.

    Args:
        group_id (str): The unique identifier for the group.
        active (bool): Whether the group is active or not.
    Raises:
        asyncpg.PostgresError: If there's an error with database operations.
    """
    try:
        async with get_db_connection() as conn:
            query = """
            UPDATE GROUPS SET active = $1, updated_at = CURRENT_TIMESTAMP
            WHERE group_id = $2
            """
            await conn.execute(query, active, group_id) # Pass boolean directly
            logger.info(f"Updated status for group {group_id} - Active: {active}")
            return True
    except asyncpg.PostgresError as e:
        logger.error(f"Error updating status for group {group_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating status for group {group_id}: {e}", exc_info=True)
        raise


async def update_group_retry_count(group_id: str, count: int):
    """
    Update a group's retry count.

    Args:
        group_id (str): The unique identifier for the group.
        count (int): The new retry count.
    Raises:
        asyncpg.PostgresError: If there's an error with database operations.
    """
    try:
        async with get_db_connection() as conn:
            query = """
            UPDATE GROUPS SET retry_count = $1, updated_at = CURRENT_TIMESTAMP
            WHERE group_id = $2
            """
            await conn.execute(query, count, group_id)
            logger.debug(f"Updated retry count for group {group_id} to {count}")
            return True
    except asyncpg.PostgresError as e:
        logger.error(f"Error updating retry count for group {group_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating retry count for group {group_id}: {e}", exc_info=True)
        raise


async def update_global_delay(delay: int):
    """Updates the global delay setting."""
    try:
        async with get_db_connection() as conn:
            # Use transaction for safety, though it's a single operation
            async with conn.transaction():
                query = "UPDATE GLOBAL_SETTINGS SET delay = $1 WHERE id = 1"
                await conn.execute(query, delay)
            logger.info(f"Global delay updated to: {delay} seconds")
            return True
    except asyncpg.PostgresError as e:
        logger.error(f"Error updating global delay: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating global delay: {e}", exc_info=True)
        raise


async def remove_group(group_id: str):
    """Remove a group from the database asynchronously."""
    try:
        async with get_db_connection() as conn:
            # Use transaction for safety
            async with conn.transaction():
                await conn.execute("DELETE FROM GROUPS WHERE group_id = $1", group_id)
            logger.info(f"Removed group {group_id} from database.")
    except asyncpg.PostgresError as e:
        logger.error(f"Error removing group {group_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error removing group {group_id}: {e}", exc_info=True)
        raise


async def get_group(group_id: str):
    """Get a specific group's data asynchronously."""
    try:
        async with get_db_connection() as conn:
            query = """
            SELECT group_id, name, last_msg_id, next_schedule, active, retry_count, current_message_index
            FROM GROUPS WHERE group_id = $1
            """
            row = await conn.fetchrow(query, group_id)

            if row:
                # Directly use values from the Record object
                return {
                    "name": row["name"],
                    "last_msg_id": row["last_msg_id"],
                    "next_schedule": row["next_schedule"], # Already datetime
                    "active": bool(row["active"]),
                    "retry_count": row["retry_count"],
                    "current_message_index": row["current_message_index"]
                }
            else:
                return None
    except asyncpg.PostgresError as e:
        logger.error(f"Error getting group data for {group_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error getting group data for {group_id}: {e}", exc_info=True)
        raise


async def get_global_messages():
    """
    Retrieve all global messages ordered by their index.

    Returns:
        list[dict]: A list of message reference dictionaries
                    (e.g., [{'chat_id': 123, 'message_id': 456, 'order_index': 0}, ...])
                    Returns an empty list if no messages are set or on error.
    Raises:
        asyncpg.PostgresError: If there's an unrecoverable error with database operations.
    """
    messages = []
    try:
        async with get_db_connection() as conn:
            query = """
            SELECT message_reference_chat_id, message_reference_message_id, order_index
            FROM GLOBAL_MESSAGES
            ORDER BY order_index ASC
            """
            rows = await conn.fetch(query)
            for row in rows:
                messages.append({
                    "chat_id": row["message_reference_chat_id"],
                    "message_id": row["message_reference_message_id"],
                    "order_index": row["order_index"]
                })
        return messages
    except asyncpg.PostgresError as e:
        logger.error(f"Error getting global messages: {e}")
        # Return empty list on error to prevent crashes in scheduler loop
        logger.warning("Returning empty global message list due to database error.")
        return []
    except Exception as e:
        logger.error(f"Unexpected error getting global messages: {e}", exc_info=True)
        logger.warning("Returning empty global message list due to unexpected error.")
        return []


async def clear_global_messages():
    """
    Delete all messages from the GLOBAL_MESSAGES table.

    Raises:
        asyncpg.PostgresError: If there's an error with database operations.
    """
    try:
        async with get_db_connection() as conn:
            # Use transaction for safety
            async with conn.transaction():
                await conn.execute("DELETE FROM GLOBAL_MESSAGES")
            logger.info("Cleared all global messages.")
            return True
    except asyncpg.PostgresError as e:
        logger.error(f"Error clearing global messages: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error clearing global messages: {e}", exc_info=True)
        raise


async def add_global_message(chat_id: int, message_id: int, index: int):
    """
    Add a single message reference to the GLOBAL_MESSAGES table.

    Args:
        chat_id (int): The chat ID where the original message exists.
        message_id (int): The message ID of the original message.
        index (int): The order index for this message.
    Raises:
        asyncpg.PostgresError: If there's an error with database operations.
    """
    try:
        async with get_db_connection() as conn:
            # Use transaction for safety
            async with conn.transaction():
                query = """
                INSERT INTO GLOBAL_MESSAGES (message_reference_chat_id, message_reference_message_id, order_index)
                VALUES ($1, $2, $3)
                """
                await conn.execute(query, chat_id, message_id, index)
            logger.debug(f"Added global message: ChatID={chat_id}, MessageID={message_id}, Index={index}")
            return True
    except asyncpg.PostgresError as e:
        # Catch potential unique constraint violation if index already exists
        if isinstance(e, asyncpg.UniqueViolationError):
             logger.error(f"Error adding global message: Index {index} already exists. {e}")
        else:
             logger.error(f"Error adding global message (Index {index}): {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error adding global message (Index {index}): {e}", exc_info=True)
        raise