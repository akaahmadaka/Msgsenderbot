# bot.py
import asyncio
import signal
import sys
import logging
import subprocess
from telegram.ext import Application, CommandHandler, filters, ConversationHandler, MessageHandler
from handlers import (
    start,  
    toggle_loop, setmsg, setdelay, status,
    startall, stopall, WAITING_FOR_MESSAGE, receive_new_message, cancel
)
from scheduler import start_scheduler, stop_scheduler, scheduler
from config import BOT_TOKEN
from db import initialize_database  # Import database functions
from logger_config import setup_logger

# Setup logger
setup_logger()
logger = logging.getLogger(__name__)

class Bot:
    def __init__(self):
        try:
            self.app = Application.builder().token(BOT_TOKEN).build()
            
            # Database will be initialized in main()
            self.is_running = False

            # Setup handlers
            # Create conversation handler for setmsg
            conv_handler = ConversationHandler(
                entry_points=[CommandHandler("setmsg", setmsg)],
                states={
                    WAITING_FOR_MESSAGE: [
                        MessageHandler(filters.TEXT & ~filters.COMMAND, receive_new_message)
                    ],
                },
                fallbacks=[CommandHandler("cancel", cancel)],
            )

            # Define regular handlers with their filters
            handlers = [
                ("start", start, filters.ChatType.PRIVATE | (filters.ChatType.GROUPS & filters.Regex(r"startloop"))),
                ("startloop", lambda update, context: toggle_loop(update, context, True), None),
                ("stoploop", lambda update, context: toggle_loop(update, context, False), None),
                ("setdelay", setdelay, None),
                ("status", status, None),
                ("startall", startall, filters.ChatType.PRIVATE),
                ("stopall", stopall, filters.ChatType.PRIVATE)
            ]

            # Add conversation handler first
            self.app.add_handler(conv_handler)

            # Register other handlers
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
            # Initialize scheduler first
            await start_scheduler()

            # Initialize and start application
            await self.app.initialize()
            logger.debug('After initialize, before start')
            await self.app.start()

            self.is_running = True

            # Initialize recovered tasks with bot instance
            recovered_count = await scheduler.initialize_pending_tasks(self.app.bot)

            # Start polling with specific updates
            logger.debug(f'Updater before polling: {self.app.updater}')
            await self.app.updater.start_polling(
                allowed_updates=["message", "callback_query"],
                drop_pending_updates=True
            )

            logger.info(f"Bot started successfully with {recovered_count} recovered tasks")

            # Keep bot alive
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

            # Stop scheduler
            await stop_scheduler()

            # Stop bot components
            if self.app.updater and self.app.updater.running:
                await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

            logger.info("Bot stopped successfully")
        except Exception as e:
            logger.error(f"Stop failed: {e}")

def install_dependencies():
    """Install dependencies from requirements.txt, upgrading if already installed."""
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-U', '-r', 'requirements.txt'])
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install dependencies: {e}")
        sys.exit(1)

async def main():
    """Main function"""
    bot = None
    loop = asyncio.get_running_loop()  # Use get_running_loop

    try:
        try:
            pass
        except ImportError:
            logger.info("The 'telegram' module is not installed. Attempting to install it...")
            install_dependencies()

        # Initialize the database (create tables)
        await initialize_database() # Added await

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

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)