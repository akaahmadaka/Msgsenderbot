# db.py
import aiosqlite
import logging
from contextlib import asynccontextmanager # Changed import
import traceback
import asyncio # Added import

DB_FILE = "bot_data.db"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager # Changed decorator
async def get_db_connection(): # Changed to async def
    """Async context manager for database connections."""
    caller = traceback.extract_stack(limit=2)[0].name
    logger.debug(f"DB Connection: Opening for {caller}...")
    conn = None # Initialize conn
    try:
        conn = await aiosqlite.connect(DB_FILE, timeout=30, isolation_level='IMMEDIATE') # Changed to await aiosqlite.connect
        await conn.execute("PRAGMA journal_mode = WAL") # Added await
        await conn.execute("PRAGMA foreign_keys = ON") # Added await
        await conn.execute("PRAGMA busy_timeout = 60000") # Added await
        await conn.execute("PRAGMA synchronous = NORMAL") # Added await
        await conn.execute("PRAGMA wal_autocheckpoint = 100") # Added await
        await conn.execute("PRAGMA cache_size = -10000") # Added await

        conn.row_factory = aiosqlite.Row # Use aiosqlite.Row
        yield conn
    except aiosqlite.Error as e: # Catch aiosqlite errors
        logger.error(f"Database connection error for {caller}: {e}")
        raise # Re-raise the exception
    finally:
        if conn:
            logger.debug(f"DB Connection: Closing for {caller}.")
            await conn.close() # Added await

async def initialize_database(): # Changed to async def
    """Initializes the database asynchronously, creating tables if they don't exist."""
    try:
        async with get_db_connection() as conn: # Changed to async with
            cursor = await conn.cursor() # Added await
            # Create GLOBAL_SETTINGS table
            await cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLOBAL_SETTINGS (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT,
                delay INTEGER,
                message_reference_chat_id INTEGER,
                message_reference_message_id INTEGER
            )
            """) # Added await
            # Create GROUPS table
            await cursor.execute("""
            CREATE TABLE IF NOT EXISTS GROUPS (
                group_id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                last_msg_id INTEGER DEFAULT 0,
                next_schedule TEXT DEFAULT '',
                active INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                error_state INTEGER DEFAULT 0
            )
            """) # Added await
            # Add default data to global_settings if it doesn't exist
            await cursor.execute("SELECT id FROM GLOBAL_SETTINGS WHERE id = 1") # Added await
            if await cursor.fetchone() is None: # Added await
                await cursor.execute("""
                INSERT INTO GLOBAL_SETTINGS (id, message, delay, message_reference_chat_id, message_reference_message_id)
                VALUES (1, 'Please set a message using /setmsg', 3600, 0, 0)""") # Added await
            await conn.commit() # Added await
            await cursor.close() # Close cursor explicitly
            logger.debug("Database initialized successfully")
    except aiosqlite.Error as e: # Catch aiosqlite errors
        logger.error(f"Database initialization error: {e}")
        raise

# Removed the if __name__ == '__main__': block