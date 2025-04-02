import aiosqlite
import asyncio
import logging
from datetime import datetime
import pytz
from functools import wraps
from db import get_db_connection
from logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_BASE_DELAY = 0.1

def with_db_retry(func):
    """Decorator to handle 'database is locked' errors with retries."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return await func(*args, **kwargs)
            except aiosqlite.OperationalError as e:
                if "database is locked" in str(e) and attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BASE_DELAY * (attempt + 1)
                    logger.warning(f"Database locked during {func.__name__}, retrying in {wait_time}s... (Attempt {attempt + 1})")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Database operational error in {func.__name__} after {attempt + 1} attempts: {e}")
                    raise
            except aiosqlite.Error as e:
                logger.error(f"Database error in {func.__name__}: {e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error in {func.__name__}: {e}", exc_info=True)
                raise
        raise aiosqlite.OperationalError(f"Failed {func.__name__} after maximum retries due to database locking.")
    return wrapper

@with_db_retry
async def get_global_settings():
    """
    Retrieve global settings (currently just delay) from the database.

    Returns:
        dict: A dictionary containing global settings (e.g., {'delay': 3600}).
              Returns default delay if not found.
    Raises:
        aiosqlite.Error: If there's an error with database operations after retries.
    """
    try:
        async with get_db_connection() as conn:
            conn.row_factory = aiosqlite.Row
            query = "SELECT delay FROM GLOBAL_SETTINGS WHERE id = 1"
            async with conn.execute(query) as cursor:
                global_settings_row = await cursor.fetchone()

            if not global_settings_row or global_settings_row["delay"] is None:
                logger.warning("Global delay not found in database, using default.")
                from config import GLOBAL_DELAY
                return {"delay": GLOBAL_DELAY}

            return {"delay": global_settings_row["delay"]}
    except aiosqlite.Error as e:
        logger.error(f"Error loading global settings from database: {e}")
        raise

@with_db_retry
async def load_data():
    """
    Load all data from the database including groups and global settings.

    Returns:
        dict: A dictionary containing global_settings and groups.
    Raises:
        aiosqlite.Error: If there's an error with database operations after retries.
    """
    try:
        groups = {}
        async with get_db_connection() as conn:
            conn.row_factory = aiosqlite.Row
            query = """
            SELECT group_id, name, last_msg_id, next_schedule, active, retry_count, current_message_index, created_at, updated_at
            FROM GROUPS
            """
            async with conn.execute(query) as cursor:
                groups_rows = await cursor.fetchall()

            for row in groups_rows:
                next_schedule_dt = None
                if row["next_schedule"]:
                    try:
                        next_schedule_dt = datetime.fromisoformat(
                            row["next_schedule"].replace('Z', '+00:00')
                        ).replace(tzinfo=pytz.UTC)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse next_schedule '{row['next_schedule']}' for group {row['group_id']}")

                groups[row["group_id"]] = {
                    "name": row["name"],
                    "last_msg_id": row["last_msg_id"],
                    "next_schedule": next_schedule_dt,
                    "active": bool(row["active"]),
                    "retry_count": row["retry_count"],
                    "current_message_index": row["current_message_index"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                }

        global_settings = await get_global_settings()
        return {"global_settings": global_settings, "groups": groups}
    except aiosqlite.Error as e:
        logger.error(f"Error loading data from database: {e}")
        raise

@with_db_retry
async def add_group(group_id, group_name):
    """
    Add a new group to the database or update an existing one.

    Args:
        group_id (str): The unique identifier for the group
        group_name (str): The name of the group
    Raises:
        aiosqlite.Error: If there's an error with database operations after retries.
    """
    try:
        async with get_db_connection() as conn:
            query = """
            INSERT OR REPLACE INTO GROUPS
            (group_id, name, last_msg_id, next_schedule, active, retry_count, created_at, updated_at)
            VALUES (?, ?,
                COALESCE((SELECT last_msg_id FROM GROUPS WHERE group_id = ?), NULL),
                COALESCE((SELECT next_schedule FROM GROUPS WHERE group_id = ?), NULL),
                COALESCE((SELECT active FROM GROUPS WHERE group_id = ?), 0),
                COALESCE((SELECT retry_count FROM GROUPS WHERE group_id = ?), 0),
                COALESCE((SELECT created_at FROM GROUPS WHERE group_id = ?), CURRENT_TIMESTAMP),
                CURRENT_TIMESTAMP
            )
            """
            await conn.execute(query, (group_id, group_name, group_id, group_id, group_id, group_id, group_id))
            await conn.commit()
            logger.debug(f"Group {group_id} ({group_name}) added or updated in database")
            return
    except aiosqlite.Error as e:
        logger.error(f"Error adding group {group_id}: {e}")
        raise

@with_db_retry
async def update_group_after_send(group_id: str, message_id: int, next_message_index: int, next_time: datetime):
    """
    Update a group's state after successfully sending a message.

    Args:
        group_id (str): The unique identifier for the group.
        message_id (int): The ID of the message just sent.
        next_message_index (int): The index of the *next* message to be sent.
        next_time (datetime): The next scheduled time (UTC).
    Raises:
        aiosqlite.Error: If there's an error with database operations after retries.
    """
    try:
        async with get_db_connection() as conn:
            next_schedule_iso = next_time.isoformat()
            query = """
            UPDATE GROUPS
            SET last_msg_id = ?, next_schedule = ?, current_message_index = ?, updated_at = CURRENT_TIMESTAMP
            WHERE group_id = ?
            """
            await conn.execute(query, (message_id, next_schedule_iso, next_message_index, group_id))
            await conn.commit()
            logger.debug(f"Updated group {group_id} after send: last_msg={message_id}, next_idx={next_message_index}, next_schedule={next_schedule_iso}")
            return True
    except aiosqlite.Error as e:
        logger.error(f"Error updating group {group_id} after send: {e}")
        raise

@with_db_retry
async def update_group_status(group_id: str, active: bool):
    """
    Update a group's active status.

    Args:
        group_id (str): The unique identifier for the group
        active (bool): Whether the group is active or not
    Raises:
        aiosqlite.Error: If there's an error with database operations after retries.
    """
    try:
        async with get_db_connection() as conn:
            query = """
            UPDATE GROUPS SET active = ?, updated_at = CURRENT_TIMESTAMP
            WHERE group_id = ?
            """
            await conn.execute(query, (int(active), group_id))
            await conn.commit()
            logger.info(f"Updated status for group {group_id} - Active: {active}")
            return True
    except aiosqlite.Error as e:
        logger.error(f"Error updating status for group {group_id}: {e}")
        raise

@with_db_retry
async def update_group_retry_count(group_id: str, count: int):
    """
    Update a group's retry count.

    Args:
        group_id (str): The unique identifier for the group
        count (int): The new retry count
    Raises:
        aiosqlite.Error: If there's an error with database operations after retries.
    """
    try:
        async with get_db_connection() as conn:
            query = """
            UPDATE GROUPS SET retry_count = ?, updated_at = CURRENT_TIMESTAMP
            WHERE group_id = ?
            """
            await conn.execute(query, (count, group_id))
            await conn.commit()
            logger.debug(f"Updated retry count for group {group_id} to {count}")
            return True
    except aiosqlite.Error as e:
        logger.error(f"Error updating retry count for group {group_id}: {e}")
        raise


@with_db_retry
async def update_global_delay(delay: int):
    try:
        async with get_db_connection() as conn:
            query = "UPDATE GLOBAL_SETTINGS SET delay = ? WHERE id = 1"
            await conn.execute(query, (delay,))
            await conn.commit()
            logger.info(f"Global delay updated to: {delay} seconds")
            return True
    except aiosqlite.Error as e:
        logger.error(f"Error updating global delay: {e}")
        raise


async def remove_group(group_id):
    """Remove a group from the database asynchronously."""
    try:
        async with get_db_connection() as conn:
            await conn.execute("DELETE FROM GROUPS WHERE group_id = ?", (group_id,))
            await conn.commit()
            logger.info(f"Removed group {group_id} from database.")
    except aiosqlite.Error as e:
        logger.error(f"Error removing group {group_id}: {e}")
        raise

async def get_group(group_id: str):
    """Get a specific group's data asynchronously."""
    try:
        async with get_db_connection() as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.cursor()
            await cursor.execute("SELECT group_id, name, last_msg_id, next_schedule, active, retry_count, current_message_index FROM GROUPS WHERE group_id = ?", (group_id,))
            row = await cursor.fetchone()
            await cursor.close()

            if row:
                next_schedule_dt = None
                if row["next_schedule"]:
                    try:
                        next_schedule_dt = datetime.fromisoformat(row["next_schedule"].replace('Z', '+00:00')).replace(tzinfo=pytz.UTC)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse next_schedule '{row['next_schedule']}' for group {group_id} in get_group")

                return {
                    "name": row["name"],
                    "last_msg_id": row["last_msg_id"],
                    "next_schedule": next_schedule_dt,
                    "active": bool(row["active"]),
                    "retry_count": row["retry_count"],
                    "current_message_index": row["current_message_index"]
                }
            else:
                return None
    except aiosqlite.Error as e:
        logger.error(f"Error getting group data for {group_id}: {e}")
        raise



@with_db_retry
async def get_global_messages():
    """
    Retrieve all global messages ordered by their index.

    Returns:
        list[dict]: A list of message reference dictionaries
                    (e.g., [{'chat_id': 123, 'message_id': 456, 'order_index': 0}, ...])
                    Returns an empty list if no messages are set.
    Raises:
        aiosqlite.Error: If there's an error with database operations after retries.
    """
    messages = []
    try:
        async with get_db_connection() as conn:
            conn.row_factory = aiosqlite.Row
            query = """
            SELECT message_reference_chat_id, message_reference_message_id, order_index
            FROM GLOBAL_MESSAGES
            ORDER BY order_index ASC
            """
            async with conn.execute(query) as cursor:
                rows = await cursor.fetchall()
            for row in rows:
                messages.append({
                    "chat_id": row["message_reference_chat_id"],
                    "message_id": row["message_reference_message_id"],
                    "order_index": row["order_index"]
                })
        return messages
    except aiosqlite.Error as e:
        logger.error(f"Error getting global messages: {e}")
        raise

@with_db_retry
async def clear_global_messages():
    """
    Delete all messages from the GLOBAL_MESSAGES table.

    Raises:
        aiosqlite.Error: If there's an error with database operations after retries.
    """
    try:
        async with get_db_connection() as conn:
            await conn.execute("DELETE FROM GLOBAL_MESSAGES")
            await conn.commit()
            logger.info("Cleared all global messages.")
            return True
    except aiosqlite.Error as e:
        logger.error(f"Error clearing global messages: {e}")
        raise

@with_db_retry
async def add_global_message(chat_id: int, message_id: int, index: int):
    """
    Add a single message reference to the GLOBAL_MESSAGES table.

    Args:
        chat_id (int): The chat ID where the original message exists.
        message_id (int): The message ID of the original message.
        index (int): The order index for this message.
    Raises:
        aiosqlite.Error: If there's an error with database operations after retries.
    """
    try:
        async with get_db_connection() as conn:
            query = """
            INSERT INTO GLOBAL_MESSAGES (message_reference_chat_id, message_reference_message_id, order_index)
            VALUES (?, ?, ?)
            """
            await conn.execute(query, (chat_id, message_id, index))
            await conn.commit()
            logger.debug(f"Added global message: ChatID={chat_id}, MessageID={message_id}, Index={index}")
            return True
    except aiosqlite.Error as e:
        logger.error(f"Error adding global message (Index {index}): {e}")
        raise