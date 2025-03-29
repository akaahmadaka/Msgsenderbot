import aiosqlite
import logging
from contextlib import asynccontextmanager
import traceback
import asyncio
from config import DB_FILE, GLOBAL_DELAY

# Logging is configured by logger_config.py
logger = logging.getLogger(__name__)

# Database connection settings
DB_TIMEOUT = 30
DB_BUSY_TIMEOUT = 60000  # 60 seconds
DB_CACHE_SIZE = -10000   # 10MB

@asynccontextmanager
async def get_db_connection():
    """
    Async context manager for database connections.

    Returns a connection with optimized settings for performance and reliability.
    Automatically closes the connection when the context is exited.
    """
    caller = traceback.extract_stack(limit=2)[0].name
    logger.debug(f"DB Connection: Opening for {caller}...")
    conn = None
    try:
        conn = await aiosqlite.connect(
            DB_FILE,
            timeout=DB_TIMEOUT,
            isolation_level='IMMEDIATE'
        )
        # Configure database for optimal performance
        await conn.execute("PRAGMA journal_mode = WAL")
        await conn.execute("PRAGMA foreign_keys = ON")
        await conn.execute(f"PRAGMA busy_timeout = {DB_BUSY_TIMEOUT}")
        await conn.execute("PRAGMA synchronous = NORMAL")
        await conn.execute("PRAGMA wal_autocheckpoint = 100")
        await conn.execute(f"PRAGMA cache_size = {DB_CACHE_SIZE}")

        conn.row_factory = aiosqlite.Row
        yield conn
    except aiosqlite.Error as e:
        logger.error(f"Database connection error for {caller}: {e}")
        raise
    finally:
        if conn:
            logger.debug(f"DB Connection: Closing for {caller}.")
            await conn.close()

async def initialize_database():
    """
    Initialize the database schema and default settings.

    Creates necessary tables if they don't exist and populates
    default values for global settings. This function should be
    called once at application startup.

    Raises:
        aiosqlite.Error: If there's an error with database operations
    """
    max_retries = 3
    retry_delay = 0.5

    for attempt in range(max_retries):
        try:
            async with get_db_connection() as conn:
                cursor = await conn.cursor()

                # Create GLOBAL_SETTINGS table
                await cursor.execute("""
                CREATE TABLE IF NOT EXISTS GLOBAL_SETTINGS (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    delay INTEGER,
                    message_reference_chat_id INTEGER,
                    message_reference_message_id INTEGER
                )
                """)

                # Create GROUPS table with improved schema
                await cursor.execute("""
                CREATE TABLE IF NOT EXISTS GROUPS (
                    group_id TEXT PRIMARY KEY NOT NULL,
                    name TEXT NOT NULL,
                    last_msg_id INTEGER DEFAULT 0,
                    next_schedule TEXT DEFAULT '',
                    active INTEGER DEFAULT 0,
                    retry_count INTEGER DEFAULT 0, -- Replaced error_count and error_state
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """)

                # Add default data to global_settings if it doesn't exist
                await cursor.execute("SELECT id FROM GLOBAL_SETTINGS WHERE id = 1")
                if await cursor.fetchone() is None:
                    await cursor.execute("""
                    INSERT INTO GLOBAL_SETTINGS (
                        id, message, delay,
                        message_reference_chat_id, message_reference_message_id
                    ) VALUES (
                        1, 'Please set a message using /setmsg', ?, 0, 0
                    )""", (GLOBAL_DELAY,))

                await conn.commit()
                await cursor.close()

                logger.info("Database initialized successfully")
                return

        except aiosqlite.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                logger.warning(f"Database locked during initialization, retrying in {retry_delay}s... (Attempt {attempt + 1})")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"Database initialization error after {attempt + 1} attempts: {e}")
                raise
        except aiosqlite.Error as e:
            logger.error(f"Database initialization error: {e}")
            raise