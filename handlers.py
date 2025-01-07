# handlers.py
from telegram import Update
from telegram.ext import ContextTypes
from utils import (
    load_data, add_group, get_global_settings, 
    update_global_settings, remove_group
)
from scheduler import (
    schedule_message, remove_scheduled_job, 
    is_running, get_active_tasks_count
)
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Admin configuration
ADMIN_IDS = [5250831809]  # Add admin IDs here

def is_admin(user_id: int) -> bool:
    """Check if user is an admin."""
    return user_id in ADMIN_IDS

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    welcome_message = (
        "👋 Welcome to Message Loop Bot!\n\n"
        "Available Commands:\n"
        "/startloop - Start message loop in group\n"
        "/stoploop - Stop message loop in group\n"
        "/setmsg <message> - Set message (Admin)\n"
        "/setdelay <seconds> - Set delay (Admin)\n"
        "/status - Check bot status (Admin)"
    )
    await update.message.reply_text(welcome_message)

async def startloop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start message loop in a group."""
    try:
        # Check if in group
        if update.message.chat.type not in ["group", "supergroup"]:
            await update.message.reply_text("❌ This command only works in groups!")
            return

        group_id = str(update.message.chat_id)
        group_name = update.message.chat.title

        # Check if already running
        if is_running(group_id):
            await update.message.reply_text("⚠️ Message loop is already running!")
            return

        # Add or update group
        add_group(group_id, group_name)
        settings = get_global_settings()

        # Start message loop
        success = await schedule_message(
            context.bot,
            group_id,
            message=settings["message"],
            delay=settings["delay"]
        )

        if success:
            await update.message.reply_text(
                f"✅ Message loop started!\n"
                f"Message: {settings['message']}\n"
                f"Delay: {settings['delay']} seconds"
            )
        else:
            await update.message.reply_text("❌ Failed to start message loop")

    except Exception as e:
        logger.error(f"Error in startloop: {e}")
        await update.message.reply_text("❌ Failed to start message loop")

async def stoploop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop message loop in a group."""
    try:
        # Check if in group
        if update.message.chat.type not in ["group", "supergroup"]:
            await update.message.reply_text("❌ This command only works in groups!")
            return

        group_id = str(update.message.chat_id)
        
        # Stop the loop
        await remove_scheduled_job(group_id)
        await update.message.reply_text("✅ Message loop stopped!")

    except Exception as e:
        logger.error(f"Error in stoploop: {e}")
        await update.message.reply_text("❌ Failed to stop message loop")

async def setmsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set global message (Admin only)."""
    try:
        # Check admin permission
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return

        # Check message content
        if not context.args:
            await update.message.reply_text("❌ Please provide a message!")
            return

        new_message = " ".join(context.args)
        settings = update_global_settings(message=new_message)
        
        await update.message.reply_text(
            f"✅ Global message updated!\n"
            f"New message: {settings['message']}"
        )

    except Exception as e:
        logger.error(f"Error in setmsg: {e}")
        await update.message.reply_text("❌ Failed to update message")

async def setdelay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set global delay (Admin only)."""
    try:
        # Check admin permission
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return

        # Check and validate delay
        if not context.args:
            await update.message.reply_text("❌ Please provide delay in seconds!")
            return

        try:
            new_delay = int(context.args[0])
            if new_delay < 10:
                await update.message.reply_text("❌ Minimum delay is 10 seconds!")
                return
        except ValueError:
            await update.message.reply_text("❌ Please provide a valid number!")
            return

        settings = update_global_settings(delay=new_delay)
        
        await update.message.reply_text(
            f"✅ Global delay updated!\n"
            f"New delay: {settings['delay']} seconds"
        )

    except Exception as e:
        logger.error(f"Error in setdelay: {e}")
        await update.message.reply_text("❌ Failed to update delay")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot status (Admin only)."""
    try:
        # Check admin permission
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return

        data = load_data()
        settings = get_global_settings()
        active_count = get_active_tasks_count()

        # Process groups
        active_groups = []
        inactive_groups = []

        for group_id, group_data in data["groups"].items():
            group_name = group_data["name"]
            next_schedule = group_data.get("next_schedule", "Not scheduled")
            
            group_info = f"{group_name} ({group_id}) - Next: {next_schedule}"
            
            if group_data["active"] and is_running(group_id):
                active_groups.append(f"🟢 {group_info}")
            else:
                inactive_groups.append(f"🔴 {group_info}")

        # Build status message
        status_message = (
            f"📊 Bot Status\n\n"
            f"Settings:\n"
            f"- Message: {settings['message']}\n"
            f"- Delay: {settings['delay']} seconds\n\n"
            f"Groups Summary:\n"
            f"- Total Groups: {len(data['groups'])}\n"
            f"- Active Groups: {active_count}\n\n"
        )

        if active_groups:
            status_message += "Active Groups:\n" + "\n".join(active_groups) + "\n\n"
        
        if inactive_groups:
            status_message += "Inactive Groups:\n" + "\n".join(inactive_groups)

        await update.message.reply_text(status_message)

    except Exception as e:
        logger.error(f"Error in status: {e}")
        await update.message.reply_text("❌ Failed to get status")

def get_handlers():
    """Return all command handlers."""
    from telegram.ext import CommandHandler
    
    return [
        CommandHandler("start", start),
        CommandHandler("startloop", startloop),
        CommandHandler("stoploop", stoploop),
        CommandHandler("setmsg", setmsg),
        CommandHandler("setdelay", setdelay),
        CommandHandler("status", status)
            ]
