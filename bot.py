import asyncio
import signal
import sys
import logging
import subprocess
import os # Added for PORT env var
import threading # Added for running Flask in thread
from flask import Flask # Added Flask
from telegram.ext import Application, CommandHandler, filters, ConversationHandler, MessageHandler
from handlers import (
    start,
    toggle_loop, setdelay, status,
    startall, stopall,
    setmsg_conversation
)
from scheduler import scheduler
from config import BOT_TOKEN
from db import initialize_database, create_pool, close_pool # Added pool functions
from logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

# --- Flask App Setup ---
flask_app = Flask(__name__)

@flask_app.route('/')
def health_check():
    # Basic health check endpoint for Render
    return "Msgsenderbot is running."
# --- End Flask App Setup ---

class Bot:
    def __init__(self):
        try:
            self.app = Application.builder().token(BOT_TOKEN).build()

            # Database will be initialized in main()
            self.is_running = False

            handlers = [
                ("start", start, filters.ChatType.PRIVATE | (filters.ChatType.GROUPS & filters.Regex(r"startloop"))),
                ("startloop", lambda update, context: toggle_loop(update, context, True), None),
                ("stoploop", lambda update, context: toggle_loop(update, context, False), None),
                ("setdelay", setdelay, None),
                ("status", status, None),
                ("startall", startall, filters.ChatType.PRIVATE),
                ("stopall", stopall, filters.ChatType.PRIVATE)
            ]

            self.app.add_handler(setmsg_conversation)

            for command, callback, filter_type in handlers:
                handler = CommandHandler(command, callback, filters=filter_type) if filter_type else CommandHandler(command, callback)
                self.app.add_handler(handler)

            self.app.add_error_handler(self.error_handler)
            logger.info("Handlers setup completed successfully")
        except Exception as e:
            logger.error(f"Handler setup failed: {e}")
            raise

    async def error_handler(self, update, context):
        """Handle errors"""
        logger.error(f"Error: {context.error}")
        if update and update.effective_message:
            await update.effective_message.reply_text("❌ Command failed")

    async def start(self):
        """Start bot"""
        try:
            await scheduler.start()

            await self.app.initialize()
            logger.debug('After initialize, before start')
            await self.app.start()

            self.is_running = True

            recovered_count = await scheduler.initialize_pending_tasks(self.app.bot)

            logger.debug(f'Updater before polling: {self.app.updater}')
            await self.app.updater.start_polling(
                allowed_updates=["message", "callback_query"],
                drop_pending_updates=True
            )

            logger.info(f"Bot started successfully with {recovered_count} recovered tasks")

            while self.is_running:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Start failed: {e}")
            self.is_running = False
            await self.stop()
            raise

    async def stop(self):
        """Stop bot"""
        try:
            self.is_running = False

            await scheduler.shutdown(self.app.bot)

            if self.app.updater and self.app.updater.running:
                await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

            logger.info("Bot stopped successfully")
        except Exception as e:
            logger.error(f"Stop failed: {e}")


async def run_bot_async():
    """Main asynchronous function to run the Telegram bot."""
    bot = None
    loop = asyncio.get_running_loop()

    try:
        await create_pool() # Create pool before initializing DB
        await initialize_database()

        bot = Bot()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig, lambda: asyncio.create_task(bot.stop())
            )
        await bot.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received shutdown signal")
        pass
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if bot:
            await bot.stop()
        await close_pool() # Close pool after bot stops

def run_flask():
    """Runs the Flask app in a separate thread."""
    port = int(os.environ.get("PORT", 8080)) # Render provides PORT env var
    logger.info(f"Starting Flask server on port {port}...")
    # Use '0.0.0.0' to be accessible externally
    # Turn off reloader and debug for production/Render
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

if __name__ == "__main__":
    # Start Flask in a background daemon thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    logger.info("Starting Telegram bot...")
    # Run the main bot logic
    try:
        asyncio.run(run_bot_async())
    except (KeyboardInterrupt, SystemExit):
         logger.info("Bot shutdown requested.")
    except Exception as e:
        logger.critical(f"Fatal error in bot execution: {e}", exc_info=True)
        sys.exit(1)
    finally:
         logger.info("Bot exiting.")
         # Note: Pool closing is handled within run_bot_async's finally block
         # Daemon thread for Flask will exit automatically when main thread exits.