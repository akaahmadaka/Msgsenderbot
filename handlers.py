# handlers.py
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from utils import load_data, save_data, add_group, get_global_settings, update_global_settings
from scheduler import schedule_message, remove_scheduled_job, is_running
import logging
from datetime import datetime, timezone, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    welcome_message = (
        "👋 Welcome! I'm a message loop bot.\n\n"
        "Available commands:\n"
        "/startloop - Start message loop in a group\n"
        "/stoploop - Stop message loop in a group\n"
        "/setmsg <message> - Set global message (private chat only)\n"
        "/setdelay <seconds> - Set global delay (private chat only, minimum 60s)\n"
        "/status - Check current settings and status"
    )
    await update.message.reply_text(welcome_message)

async def startloop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /startloop command."""
    try:
        if update.message.chat.type not in ["group", "supergroup"]:
            await update.message.reply_text("❌ This command only works in groups!")
            return

        group_id = str(update.message.chat_id)
        group_name = update.message.chat.title

        # Check if loop is already running
        if is_running(group_id):
            await update.message.reply_text("⚠️ Message loop is already running!")
            return

        # Add group to data file if not already present
        group_data = add_group(group_id, group_name)
        
        # Get global settings
        global_settings = get_global_settings()

        # Start the message loop
        data = load_data()
        data["groups"][group_id]["loop_running"] = True
        save_data(data)
        
        success = await schedule_message(
            context.bot, 
            group_id,
            message=global_settings["message"],
            delay=global_settings["delay"]
        )

        if success:
            await update.message.reply_text(
                f"✅ Message loop started!\n"
                f"Message: {global_settings['message']}\n"
                f"Delay: {global_settings['delay']} seconds"
            )
        else:
            data["groups"][group_id]["loop_running"] = False
            save_data(data)
            await update.message.reply_text("❌ Failed to start message loop.")

    except Exception as e:
        logger.error(f"Error in startloop for group {group_id}: {e}")
        await update.message.reply_text("❌ An error occurred while starting the loop.")
        
        # Cleanup on error
        try:
            data = load_data()
            if group_id in data["groups"]:
                data["groups"][group_id]["loop_running"] = False
                save_data(data)
            await remove_scheduled_job(group_id)
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {cleanup_error}")

async def stoploop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stoploop command."""
    try:
        if update.message.chat.type not in ["group", "supergroup"]:
            await update.message.reply_text("❌ This command only works in groups!")
            return

        group_id = str(update.message.chat_id)

        # Check if loop is actually running
        if not is_running(group_id):
            await update.message.reply_text("ℹ️ No active loop to stop.")
            # Ensure data file is in sync
            data = load_data()
            if group_id in data["groups"]:
                data["groups"][group_id]["loop_running"] = False
                save_data(data)
            return

        # Stop the message loop
        data = load_data()
        if group_id in data["groups"]:
            data["groups"][group_id]["loop_running"] = False
            save_data(data)
            await remove_scheduled_job(group_id)
            await update.message.reply_text("✅ Message loop stopped!")
        else:
            await update.message.reply_text("❌ Group not found in database.")

    except Exception as e:
        logger.error(f"Error in stoploop for group {group_id}: {e}")
        await update.message.reply_text("❌ An error occurred while stopping the loop.")

async def setmsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setmsg command (private chat only)."""
    try:
        if update.message.chat.type != "private":
            await update.message.reply_text("❌ This command is only available in private chat.")
            return

        if not context.args:
            await update.message.reply_text("❌ Please provide a message.")
            return

        new_message = " ".join(context.args)
        
        # Update global message setting
        settings = update_global_settings(message=new_message)
        
        # Reschedule all running groups with new message
        data = load_data()
        updated_groups = 0
        failed_groups = 0
        
        for group_id, group_data in data["groups"].items():
            if group_data["loop_running"] and is_running(group_id):
                try:
                    await schedule_message(
                        context.bot,
                        group_id,
                        message=settings["message"],
                        delay=settings["delay"]
                    )
                    updated_groups += 1
                except Exception as e:
                    logger.error(f"Failed to update message for group {group_id}: {e}")
                    failed_groups += 1
        
        status_message = (
            f"✅ Message updated globally!\n"
            f"New message: {new_message}\n"
            f"Updated groups: {updated_groups}"
        )
        if failed_groups > 0:
            status_message += f"\nFailed updates: {failed_groups}"
        
        await update.message.reply_text(status_message)
        
    except Exception as e:
        logger.error(f"Failed to set message: {e}")
        await update.message.reply_text("❌ Failed to update message.")

async def setdelay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setdelay command (private chat only)."""
    try:
        if update.message.chat.type != "private":
            await update.message.reply_text("❌ This command is only available in private chat.")
            return

        if not context.args:
            await update.message.reply_text("❌ Please provide a delay in seconds (minimum 60).")
            return

        try:
            new_delay = int(context.args[0])
            if new_delay < 60:
                await update.message.reply_text("❌ Delay must be at least 60 seconds.")
                return
        except ValueError:
            await update.message.reply_text("❌ Please provide a valid number for delay.")
            return
            
        # Update global delay setting
        settings = update_global_settings(delay=new_delay)
        
        # Reschedule all running groups with new delay
        data = load_data()
        updated_groups = 0
        failed_groups = 0
        
        for group_id, group_data in data["groups"].items():
            if group_data["loop_running"] and is_running(group_id):
                try:
                    await schedule_message(
                        context.bot,
                        group_id,
                        message=settings["message"],
                        delay=settings["delay"]
                    )
                    updated_groups += 1
                except Exception as e:
                    logger.error(f"Failed to update delay for group {group_id}: {e}")
                    failed_groups += 1
        
        status_message = (
            f"✅ Delay updated globally!\n"
            f"New delay: {new_delay} seconds\n"
            f"Updated groups: {updated_groups}"
        )
        if failed_groups > 0:
            status_message += f"\nFailed updates: {failed_groups}"
        
        await update.message.reply_text(status_message)
        
    except Exception as e:
        logger.error(f"Failed to set delay: {e}")
        await update.message.reply_text("❌ Failed to update delay.")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command."""
    try:
        settings = get_global_settings()
        data = load_data()
        
        active_groups = sum(1 for group_id in data["groups"] if is_running(str(group_id)))
        total_groups = len(data["groups"])
        
        status_message = (
            "📊 Current Status:\n\n"
            f"Message: {settings['message']}\n"
            f"Delay: {settings['delay']} seconds\n"
            f"Active Groups: {active_groups}/{total_groups}"
        )
        
        await update.message.reply_text(status_message)
    except Exception as e:
        logger.error(f"Error in status command: {e}")
        await update.message.reply_text("❌ Failed to get status.")

def get_handlers():
    """Return all command handlers."""
    return [
        CommandHandler("start", start),
        CommandHandler("startloop", startloop),
        CommandHandler("stoploop", stoploop),
        CommandHandler("setmsg", setmsg),
        CommandHandler("setdelay", setdelay),
        CommandHandler("status", status),
    ]