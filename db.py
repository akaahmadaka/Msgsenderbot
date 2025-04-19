import asyncpg
import logging
import os
import traceback
import asyncio
from contextlib import asynccontextmanager
from config import GLOBAL_DELAY # Keep GLOBAL_DELAY for default setting

# Logging is configured by logger_config.py
logger = logging.getLogger(__name__)

# Global connection pool
_pool = None

async def create_pool():
    """Creates the asyncpg connection pool."""
    global _pool
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.critical("DATABASE_URL environment variable not set. Cannot connect to PostgreSQL.")
        raise ValueError("DATABASE_URL environment variable not set.")

    min_conn = 1
    max_conn = 10 # Adjust as needed

    logger.info(f"Creating PostgreSQL connection pool (min={min_conn}, max={max_conn})...")
    try:
        _pool = await asyncpg.create_pool(
            dsn=database_url,
            min_size=min_conn,
            max_size=max_conn,
            # Add other pool options if needed, e.g., command_timeout
        )
        logger.info("PostgreSQL connection pool created successfully.")
    except Exception as e:
        logger.critical(f"Failed to create PostgreSQL connection pool: {e}")
        raise

async def close_pool():
    """Closes the asyncpg connection pool."""
    global _pool
    if _pool:
        logger.info("Closing PostgreSQL connection pool...")
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL connection pool closed.")

@asynccontextmanager
async def get_db_connection():
    """
    Async context manager for acquiring a connection from the pool.
    """
    global _pool
    if not _pool:
        # Attempt to create the pool if it doesn't exist (e.g., during initialization)
        # In a real app, pool creation should ideally happen once at startup.
        logger.warning("Connection pool not initialized. Attempting to create now...")
        await create_pool()
        if not _pool: # Check again after creation attempt
             raise RuntimeError("Database connection pool is not available.")


    caller = traceback.extract_stack(limit=2)[0].name
    logger.debug(f"DB Connection: Acquiring for {caller}...")
    conn = None
    try:
        # Acquire connection from pool
        conn = await _pool.acquire()
        logger.debug(f"DB Connection: Acquired for {caller}.")
        yield conn
    except asyncpg.PostgresError as e:
        logger.error(f"Database connection/operation error for {caller}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during DB connection handling for {caller}: {e}")
        raise
    finally:
        if conn:
            logger.debug(f"DB Connection: Releasing for {caller}.")
            # Release connection back to pool
            await _pool.release(conn)
            logger.debug(f"DB Connection: Released for {caller}.")


async def initialize_database():
    """
    Initialize the PostgreSQL database schema and default settings.

    Creates necessary tables if they don't exist and populates
    default values for global settings. This function should be
    called once at application startup.

    Raises:
        asyncpg.PostgresError: If there's an error with database operations.
        ValueError: If DATABASE_URL is not set.
    """
    # Ensure pool is created before initializing
    if not _pool:
        await create_pool()

    max_retries = 3
    retry_delay = 1.0 # Start with a 1-second delay

    for attempt in range(max_retries):
        try:
            async with get_db_connection() as conn:
                # Use transaction for schema changes
                async with conn.transaction():
                    logger.info("Initializing database schema...")

                    # Create GLOBAL_SETTINGS table
                    await conn.execute("""
                    CREATE TABLE IF NOT EXISTS GLOBAL_SETTINGS (
                        id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1), -- Ensure only one row
                        delay INTEGER NOT NULL
                    )
                    """)

                    # Create GLOBAL_MESSAGES table
                    await conn.execute("""
                    CREATE TABLE IF NOT EXISTS GLOBAL_MESSAGES (
                        id SERIAL PRIMARY KEY,
                        message_reference_chat_id BIGINT NOT NULL,
                        message_reference_message_id BIGINT NOT NULL,
                        order_index INTEGER NOT NULL UNIQUE
                    )
                    """)
                    # Create index for faster lookups
                    await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_global_messages_order ON GLOBAL_MESSAGES(order_index);
                    """)

                    # Create GROUPS table
                    await conn.execute("""
                    CREATE TABLE IF NOT EXISTS GROUPS (
                        group_id TEXT PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL,
                        last_msg_id BIGINT DEFAULT 0,
                        next_schedule TIMESTAMPTZ, -- Use TIMESTAMPTZ for timezone awareness
                        active BOOLEAN DEFAULT FALSE, -- Use BOOLEAN type
                        retry_count INTEGER DEFAULT 0,
                        current_message_index INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    )
                    """)

                    # Add click_count column to GROUPS if it doesn't exist
                    await conn.execute("""
                    ALTER TABLE GROUPS
                    ADD COLUMN IF NOT EXISTS click_count INTEGER DEFAULT 0 NOT NULL;
                    """)
                    logger.info("Ensured 'click_count' column exists in GROUPS table.")

                    # Add default data to global_settings if it doesn't exist
                    settings_exist = await conn.fetchrow("SELECT id FROM GLOBAL_SETTINGS WHERE id = 1")
                    if settings_exist is None:
                        await conn.execute("""
                        INSERT INTO GLOBAL_SETTINGS (id, delay) VALUES (1, $1)
                        """, GLOBAL_DELAY)
                        logger.info(f"Inserted default GLOBAL_DELAY: {GLOBAL_DELAY}")

                logger.info("Database schema initialized successfully.")
                return # Success, exit retry loop

        except (asyncpg.PostgresConnectionError, ConnectionRefusedError, OSError) as e:
             # Errors typically occurring during connection attempts
            if attempt < max_retries - 1:
                logger.warning(f"Database connection error during initialization (Attempt {attempt + 1}/{max_retries}), retrying in {retry_delay:.1f}s... Error: {e}")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2 # Exponential backoff
            else:
                logger.critical(f"Database initialization failed after {max_retries} attempts due to connection error: {e}")
                raise
        except asyncpg.PostgresError as e:
            logger.error(f"Database schema initialization error: {e}")
            raise # Raise other PostgreSQL errors immediately
        except Exception as e:
             logger.critical(f"Unexpected error during database initialization: {e}")
             raise # Raise unexpected errors immediately

    # This part should ideally not be reached if retries fail, as exceptions are raised
    logger.critical(f"Database initialization failed after {max_retries} attempts.")
    raise RuntimeError("Database initialization failed after maximum retries.")