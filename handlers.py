# handlers.py
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from utils import load_data, save_data, add_group, get_global_settings, update_global_settings, remove_group
from scheduler import schedule_message, remove_scheduled_job, is_running
import logging
from datetime import datetime, timezone, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define admin IDs (replace with actual admin IDs)
ADMIN_IDS = [5250831809]  # Add your admin ID here

def is_admin(user_id: int) -> bool:
    """Check if user is an admin."""
    return user_id in ADMIN_IDS

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    welcome_message = (
        "👋 Welcome! I'm a message loop bot.\n\n"
        "Available commands:\n"
        "/startloop - Start message loop in a group\n"
        "/stoploop - Stop message loop in a group\n"
        "/setmsg <message> - Set global message (admin only)\n"
        "/setdelay <seconds> - Set global delay (admin only)\n"
        "/status - Check current settings and status (admin only)"
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
        data["groups"][group_id]["error_count"] = 0  # Reset error count
        data["groups"][group_id]["last_start"] = datetime.now(timezone.utc).isoformat()
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

async def stoploop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stoploop command."""
    try:
        if update.message.chat.type not in ["group", "supergroup"]:
            await update.message.reply_text("❌ This command only works in groups!")
            return

        group_id = str(update.message.chat_id)

        data = load_data()
        if group_id in data["groups"]:
            data["groups"][group_id]["loop_running"] = False
            data["groups"][group_id]["stop_time"] = datetime.now(timezone.utc).isoformat()
            save_data(data)
            await remove_scheduled_job(group_id)
            await update.message.reply_text("✅ Message loop stopped!")
        else:
            await update.message.reply_text("❌ Group not found in database.")

    except Exception as e:
        logger.error(f"Error in stoploop for group {group_id}: {e}")
        await update.message.reply_text("❌ An error occurred while stopping the loop.")

async def setmsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setmsg command (admin only)."""
    try:
        # Check if user is admin
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ This command is only available to administrators.")
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
    """Handle /setdelay command (admin only)."""
    try:
        # Check if user is admin
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ This command is only available to administrators.")
            return

        if not context.args:
            await update.message.reply_text("❌ Please provide a delay in seconds (minimum 10).")
            return

        try:
            new_delay = int(context.args[0])
            if new_delay < 10:
                await update.message.reply_text("❌ Delay must be at least 10 seconds.")
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
    """Handle /status command (admin only)."""
    try:
        # Check if user is admin
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ This command is only available to administrators.")
            return

        settings = get_global_settings()
        data = load_data()
        current_time = datetime.now(timezone.utc)
        
        # Process groups and remove inactive ones
        active_groups = []
        inactive_groups = []
        groups_to_remove = []

        for group_id, group_data in data["groups"].items():
            # Check for groups with too many errors
            error_count = group_data.get("error_count", 0)
            if error_count >= 5:  # Maximum error threshold
                groups_to_remove.append(group_id)
                continue

            group_name = group_data.get("name", "Unknown Group")
            is_active = group_data.get("loop_running", False) and is_running(group_id)
            next_run = group_data.get("next_run")
            
            if next_run:
                try:
                    next_run_time = datetime.fromisoformat(next_run)
                    time_until_next = next_run_time - current_time
                    next_run_str = f"Next: {time_until_next.total_seconds():.0f}s"
                except (ValueError, TypeError):
                    next_run_str = "Next: Unknown"
            else:
                next_run_str = "Not scheduled"

            group_info = f"{group_name} ({group_id}) - {next_run_str}"
            
            if is_active:
                active_groups.append(f"🟢 {group_info}")
            else:
                inactive_groups.append(f"🔴 {group_info}")

        # Remove groups with too many errors
        for group_id in groups_to_remove:
            remove_group(group_id)
            logger.info(f"Removed group {group_id} due to excessive errors")

        # Current UTC time
        current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        status_message = (
            f"📊 Bot Status Report\n"
            f"Time: {current_time_str}\n\n"
            f"Global Settings:\n"
            f"- Message: {settings['message']}\n"
            f"- Delay: {settings['delay']} seconds\n\n"
            f"Groups Summary:\n"
            f"- Total Groups: {len(data['groups'])}\n"
            f"- Active: {len(active_groups)}\n"
            f"- Inactive: {len(inactive_groups)}\n\n"
        )

        if active_groups:
            status_message += "Active Groups:\n" + "\n".join(active_groups) + "\n\n"
        
        if inactive_groups:
            status_message += "Inactive Groups:\n" + "\n".join(inactive_groups)

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
