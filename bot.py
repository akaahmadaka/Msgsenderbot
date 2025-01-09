# bot.py
import asyncio
import signal
import sys
import logging
from telegram.ext import Application, CommandHandler, filters, ConversationHandler, MessageHandler
from handlers import (
    start, startloop, stoploop,
    setmsg, setdelay, status,
    startall, stopall, WAITING_FOR_MESSAGE, receive_new_message, cancel
)
from scheduler import start_scheduler, stop_scheduler, scheduler
from config import BOT_TOKEN
from utils import get_global_settings

# Disable all external loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

# Configure simple logging
class SimpleFormatter(logging.Formatter):
    def format(self, record):
        return f"{record.getMessage()}"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(SimpleFormatter())

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler]
)
logger = logging.getLogger(__name__)

class Bot:
    def __init__(self):
        """Initialize bot"""
        try:
            self.app = Application.builder().token(BOT_TOKEN).build()
            self.is_running = False
            self.setup_handlers()
        except Exception as e:
            logger.error(f"Init failed: {e}")
            raise

    def setup_handlers(self):
        """Setup command handlers"""
        try:
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
                ("startloop", startloop, None),
                ("stoploop", stoploop, None),
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
            await self.app.start()

            self.is_running = True

            # Initialize recovered tasks with bot instance
            recovered_count = await scheduler.initialize_pending_tasks(self.app.bot)

            # Start polling with specific updates
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

def setup_signal_handlers(bot):
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        bot.is_running = False

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, signal_handler)

async def main():
    """Main function"""
    bot = None
    try:
        bot = Bot()
        setup_signal_handlers(bot)
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