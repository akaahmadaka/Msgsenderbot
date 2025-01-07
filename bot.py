# bot.py
import logging
import asyncio
import signal
import sys
import os
from datetime import datetime
from telegram.ext import Application, CommandHandler
from handlers import startloop, stoploop, setmsg, setdelay
from scheduler import start_scheduler, stop_scheduler
from config import BOT_TOKEN

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class Bot:
    def __init__(self):
        """Initialize the bot with application builder"""
        try:
            self.app = Application.builder().token(BOT_TOKEN).build()
            self.is_running = False
            self.setup_handlers()
            logger.info("Bot initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize bot: {e}")
            raise

    def setup_handlers(self):
        """Set up command handlers"""
        try:
            # Command handlers
            self.app.add_handler(CommandHandler("startloop", startloop))
            self.app.add_handler(CommandHandler("stoploop", stoploop))
            self.app.add_handler(CommandHandler("setmsg", setmsg))
            self.app.add_handler(CommandHandler("setdelay", setdelay))
            
            # Error handler
            self.app.add_error_handler(self.error_handler)
            logger.info("Handlers setup completed")
        except Exception as e:
            logger.error(f"Failed to setup handlers: {e}")
            raise

    async def error_handler(self, update, context):
        """Handle errors in updates"""
        logger.error(f"Update {update} caused error {context.error}")
        if update and update.effective_message:
            error_message = "An error occurred while processing your command."
            await update.effective_message.reply_text(error_message)

    async def start(self):
        """Start the bot and scheduler"""
        try:
            # Start the scheduler
            start_scheduler()
            logger.info("Scheduler started")

            # Initialize and start the bot
            await self.app.initialize()
            await self.app.start()
            
            # Start polling
            self.is_running = True
            await self.app.updater.start_polling(
                allowed_updates=["message", "callback_query"],
                drop_pending_updates=True
            )
            
            logger.info("Bot started successfully!")
            
            # Keep the bot running
            while self.is_running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error during bot startup: {e}")
            self.is_running = False
            await self.stop()
            raise

    async def stop(self):
        """Stop the bot and cleanup resources"""
        try:
            logger.info("Stopping bot...")
            self.is_running = False
            
            # Stop the scheduler
            stop_scheduler()
            logger.info("Scheduler stopped")

            # Stop the bot components in order
            try:
                if self.app.updater and self.app.updater.running:
                    await self.app.updater.stop()
                    logger.info("Updater stopped")
            except Exception as e:
                logger.warning(f"Error stopping updater: {e}")

            try:
                await self.app.stop()
                await self.app.shutdown()
                logger.info("Application stopped")
            except Exception as e:
                logger.warning(f"Error stopping application: {e}")

            logger.info("Bot stopped successfully")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

def setup_signal_handlers(bot):
    """Set up signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        bot.is_running = False

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, signal_handler)
        logger.info("Signal handlers setup completed")
    except Exception as e:
        logger.error(f"Error setting up signal handlers: {e}")

async def main():
    """Main function to run the bot"""
    bot = None
    try:
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Create and start the bot
        bot = Bot()
        setup_signal_handlers(bot)
        
        # Log startup information
        logger.info(f"Bot starting at {datetime.utcnow().isoformat()}")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Running as user: {os.getenv('USER', 'unknown')}")
        
        # Start the bot
        await bot.start()
        
    except (KeyboardInterrupt, SystemExit):
        logger.info("\nReceived shutdown signal")
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
    finally:
        if bot:
            await bot.stop()
        logger.info("Bot shutdown complete")

if __name__ == "__main__":
    try:
        # Create necessary directories
        os.makedirs('logs', exist_ok=True)
        os.makedirs('data', exist_ok=True)
        
        # Run the bot
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot shutdown complete!")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
