import asyncio
import signal
import sys
import logging
import subprocess
from telegram.ext import Application, CommandHandler, filters, ConversationHandler, MessageHandler
from handlers import (
    start,
    toggle_loop, setdelay, status,
    startall, stopall,
    setmsg_conversation
)
from scheduler import scheduler
from config import BOT_TOKEN
from db import initialize_database
from logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

class Bot:
    def __init__(self):
        try:
            self.app = Application.builder().token(BOT_TOKEN).build()

            # Database will be initialized in main()
            self.is_running = False

            handlers = [
                ("start", start, filters.ChatType.PRIVATE | (filters.ChatType.GROUPS & filters.Regex(r"GetVideo"))),
                ("getvideo", lambda update, context: toggle_loop(update, context, True), None),
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


async def main():
    """Main function"""
    bot = None
    loop = asyncio.get_running_loop()
    shutdown_requested = False # Flag to signal clean shutdown

    def signal_handler():
        nonlocal shutdown_requested
        if not shutdown_requested:
            logger.info("Shutdown signal received. Attempting graceful stop...")
            shutdown_requested = True
            # Signal the main loop to stop, and trigger bot.stop() if running
            if bot and bot.is_running:
                 asyncio.create_task(bot.stop())
        else:
            logger.warning("Shutdown already in progress.")

    try:
        await initialize_database()
        logger.info("Database initialized.")

        # Register signal handlers *before* the loop
        for sig in (signal.SIGTERM, signal.SIGINT):
             loop.add_signal_handler(sig, signal_handler)
        logger.info("Signal handlers registered.")

        while not shutdown_requested:
            logger.info("--- Starting new bot instance ---")
            bot = None # Ensure we create a new instance each time
            try:
                bot = Bot()
                logger.info("Bot instance created. Starting...")
                # bot.start() contains its own running loop. Await it here.
                # If it exits (cleanly or crash), the code below runs.
                await bot.start()

                # If bot.start() returns cleanly:
                if shutdown_requested:
                    logger.info("Bot stopped cleanly via signal.")
                    break # Exit the while loop gracefully
                else:
                    # Implies an unexpected stop within bot.start() or bot.stop() called manually
                    logger.warning("bot.start() exited without shutdown signal. Restarting...")

            except (KeyboardInterrupt, SystemExit):
                 logger.info("KeyboardInterrupt/SystemExit caught in main loop. Initiating shutdown.")
                 shutdown_requested = True # Ensure loop terminates
                 break
            except Exception as e:
                logger.exception(f"Error during bot execution/startup: {e}. Restarting...", exc_info=e)
                # Attempt to clean up the failed bot instance
                if bot:
                    logger.info("Attempting to stop failed bot instance...")
                    try:
                        await asyncio.wait_for(bot.stop(), timeout=10.0)
                    except asyncio.TimeoutError:
                        logger.error("Timeout waiting for failed bot instance to stop.")
                    except Exception as stop_e:
                        logger.error(f"Error stopping failed bot instance: {stop_e}")
            finally:
                # Runs whether bot.start() exits cleanly, crashes, or KeyboardInterrupt
                if bot and bot.is_running and shutdown_requested:
                     logger.info("Ensuring bot is stopped in finally block due to shutdown request...")
                     try:
                         await asyncio.wait_for(bot.stop(), timeout=10.0)
                     except Exception as final_stop_e:
                         logger.error(f"Error during final stop attempt: {final_stop_e}")
                bot = None # Dereference bot object before next loop or exit

            if not shutdown_requested:
                wait_time = 15 # Seconds
                logger.info(f"Waiting {wait_time} seconds before attempting restart...")
                try:
                    await asyncio.sleep(wait_time)
                except asyncio.CancelledError:
                     logger.info("Restart wait interrupted by shutdown signal.")
                     shutdown_requested = True # Ensure loop terminates

    except Exception as setup_error:
        # Catch errors during initial setup outside the loop (e.g., DB init)
        logger.exception(f"Fatal error during initial setup: {setup_error}", exc_info=setup_error)
    finally:
        logger.info("--- Main application loop finished ---")
        # Final cleanup outside the bot instance itself can go here

if __name__ == "__main__":
    # The main() function now handles its own exceptions and restart loop.
    # We just run it. If it exits, the script exits.
    # Errors within main() should be logged there.
    asyncio.run(main())
    logger.info("Application has finished execution.")
    sys.exit(0) # Explicitly exit with success code after main finishes