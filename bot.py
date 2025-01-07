# bot.py
from telegram.ext import Application, CommandHandler
from handlers import startloop, stoploop, setmsg, setdelay
from scheduler import start_scheduler, stop_scheduler
from config import BOT_TOKEN
import asyncio
import signal
import sys

class Bot:
    def __init__(self):
        self.app = Application.builder().token(BOT_TOKEN).build()
        self.setup_handlers()
        
    def setup_handlers(self):
        """Set up command handlers"""
        self.app.add_handler(CommandHandler("startloop", startloop))
        self.app.add_handler(CommandHandler("stoploop", stoploop))
        self.app.add_handler(CommandHandler("setmsg", setmsg))
        self.app.add_handler(CommandHandler("setdelay", setdelay))

    async def start(self):
        """Start the bot"""
        # Start the scheduler
        start_scheduler()
        
        # Start the bot
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(allowed_updates=["message"])
        
        print("Bot started successfully!")
        
        # Keep the bot running
        try:
            await self.app.updater.running
        finally:
            await self.stop()

    async def stop(self):
        """Stop the bot"""
        try:
            # Stop the scheduler
            stop_scheduler()
            
            # Stop the bot
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
        except Exception as e:
            print(f"Error during shutdown: {e}")

def handle_signals():
    """Set up signal handlers"""
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, signal.default_int_handler)
    except Exception as e:
        print(f"Error setting up signal handlers: {e}")

async def main():
    """Main function"""
    # Set up signal handlers
    handle_signals()
    
    # Create and start the bot
    bot = Bot()
    try:
        await bot.start()
    except (KeyboardInterrupt, SystemExit):
        print("\nReceived shutdown signal, closing bot...")
        await bot.stop()
    except Exception as e:
        print(f"Critical error: {e}")
        await bot.stop()
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\nBot shutdown complete!")