import sqlite3
import logging
from contextlib import contextmanager
import traceback

DB_FILE = "bot_data.db"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    caller = traceback.extract_stack(limit=2)[0].name  # Get the name of the calling function
    logger.debug(f"DB Connection: Opening for {caller}...")
    conn = sqlite3.connect(DB_FILE, timeout=30, isolation_level='IMMEDIATE')
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 60000")
    conn.execute("PRAGMA synchronous = NORMAL")  # More frequent checkpoints
    conn.execute("PRAGMA wal_autocheckpoint = 100")
    conn.execute("PRAGMA cache_size = -10000")  # 10MB cache
    try:
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        logger.debug(f"DB Connection: Closing for {caller}.")
        conn.close()

def initialize_database():
    """Initializes the database, creating tables if they don't exist."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Create GLOBAL_SETTINGS table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLOBAL_SETTINGS (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT,
                delay INTEGER,
                message_reference_chat_id INTEGER,
                message_reference_message_id INTEGER
            )
            """)
            # Create GROUPS table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS GROUPS (
                group_id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                last_msg_id INTEGER DEFAULT 0,
                next_schedule TEXT DEFAULT '',
                active INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                error_state INTEGER DEFAULT 0
            )
            """)
            # Add default data to global_settings if it doesn't exist
            cursor.execute("SELECT id FROM GLOBAL_SETTINGS WHERE id = 1")
            if cursor.fetchone() is None:
                cursor.execute("""
                INSERT INTO GLOBAL_SETTINGS (id, message, delay, message_reference_chat_id, message_reference_message_id)
                VALUES (1, 'Please set a message using /setmsg', 3600, 0, 0)""")
            conn.commit()
            logger.debug("Database initialized successfully")
    except sqlite3.Error as e:
        logger.error(f"Database initialization error: {e}")
        raise

if __name__ == '__main__':
    initialize_database()