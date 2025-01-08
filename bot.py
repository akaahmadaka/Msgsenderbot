# bot.py
import asyncio
import signal
import sys
import logging
from telegram.ext import Application, CommandHandler
from handlers import start, startloop, stoploop, setmsg, setdelay, status
from scheduler import start_scheduler, stop_scheduler
from config import BOT_TOKEN

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
            self.app.add_handler(CommandHandler("start", start))
            self.app.add_handler(CommandHandler("startloop", startloop))
            self.app.add_handler(CommandHandler("stoploop", stoploop))
            self.app.add_handler(CommandHandler("setmsg", setmsg))
            self.app.add_handler(CommandHandler("setdelay", setdelay))
            self.app.add_handler(CommandHandler("status", status))
            self.app.add_error_handler(self.error_handler)
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
            await start_scheduler()
            await self.app.initialize()
            await self.app.start()
            
            self.is_running = True
            await self.app.updater.start_polling(
                allowed_updates=["message"],
                drop_pending_updates=True
            )
            
            logger.info("Bot running")
            
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
            
            await stop_scheduler()
            
            if self.app.updater and self.app.updater.running:
                await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
            
            logger.info("Bot stopped")
        except Exception as e:
            logger.error(f"Stop failed: {e}")

def setup_signal_handlers(bot):
    """Setup signal handlers"""
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