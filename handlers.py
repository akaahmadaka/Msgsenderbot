# handlers.py
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes
from utils import (
    load_data, add_group, get_global_settings, 
    update_global_settings, update_group_status, remove_group
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
    is_admin_user = is_admin(update.effective_user.id)
    
    base_commands = (
        "👋 Welcome to Message Loop Bot!\n\n"
        "Available Commands:\n"
        "/startloop - Start message loop in group\n"
        "/stoploop - Stop message loop in group\n"
    )
    
    admin_commands = (
        "/setmsg <message> - Set message (Admin)\n"
        "/setdelay <seconds> - Set delay (Admin)\n"
        "/status - Check bot status (Admin)\n"
        "/startall - Start all stopped groups (Admin, private)\n"
        "/stopall - Stop all running groups (Admin, private)"
    )
    
    welcome_message = base_commands + (admin_commands if is_admin_user else "")
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
        
        # Load data and check if group exists
        data = load_data()
        if group_id not in data["groups"]:
            logger.info(f"Stop command failed - Group {group_id} not found in database")
            await update.message.reply_text("❌ No active loop found!")
            return

        # Update group status to inactive
        update_group_status(group_id, False)
        logger.info(f"Group {group_id} status updated to inactive")

        # Stop the loop
        await remove_scheduled_job(group_id)
        logger.info(f"Message loop stopped for group {group_id}")

        # Send confirmation
        await update.message.reply_text("✅ Message loop stopped!")

    except Exception as e:
        logger.error(f"Stop command failed - {str(e)}")
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

async def startall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start message loop in all manually stopped groups (Admin only)."""
    try:
        # Check admin permission and private chat
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return
            
        if update.message.chat.type != "private":
            await update.message.reply_text("❌ This command only works in private chat!")
            return

        data = load_data()
        settings = get_global_settings()
        started_count = 0
        
        # Get list of manually stopped groups
        for group_id, group in data["groups"].items():
            if not group.get("active", False) and not group.get("error_state", False):
                success = await schedule_message(
                    context.bot,
                    group_id,
                    message=settings["message"],
                    delay=settings["delay"]
                )
                if success:
                    started_count += 1
                    logger.info(f"Restarted loop in group {group['name']}")

        if started_count > 0:
            await update.message.reply_text(
                f"✅ Successfully started message loop in {started_count} groups!"
            )
        else:
            await update.message.reply_text("ℹ️ No manually stopped groups found")

    except Exception as e:
        logger.error(f"Start all command failed - {str(e)}")
        await update.message.reply_text("❌ Failed to start groups")

async def stopall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop message loop in all manually started groups (Admin only)."""
    try:
        # Check admin permission and private chat
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return
            
        if update.message.chat.type != "private":
            await update.message.reply_text("❌ This command only works in private chat!")
            return

        data = load_data()
        stopped_count = 0
        
        # Get list of manually started groups
        for group_id, group in data["groups"].items():
            if group.get("active", False):
                # Update group status to inactive
                update_group_status(group_id, False)
                # Stop the loop
                await remove_scheduled_job(group_id)
                stopped_count += 1
                logger.info(f"Stopped loop in group {group['name']}")

        if stopped_count > 0:
            await update.message.reply_text(
                f"✅ Successfully stopped message loop in {stopped_count} groups!"
            )
        else:
            await update.message.reply_text("ℹ️ No active groups found")

    except Exception as e:
        logger.error(f"Stop all command failed - {str(e)}")
        await update.message.reply_text("❌ Failed to stop groups")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot status (Admin only)."""
    try:
        # Check admin permission
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return

        data = load_data()
        
        # Count groups
        active_count = sum(1 for group in data["groups"].values() if group.get("active", False))
        total_count = len(data["groups"])

        # Create status message with emojis and formatting
        status_msg = (
            "📊 *Bot Status*\n\n"
            f"📈 Groups: {total_count} │ Active: {active_count}\n\n"
            "*Group Status:*\n"
        )

        # Separate active and stopped groups
        running_groups = []
        stopped_groups = []
        
        for group_id, group in data["groups"].items():
            group_name = group.get("name", "Unknown Group")
            if group.get("active", False):
                running_groups.append(f"🟢 {group_name}")
            else:
                stopped_groups.append(f"🔴 {group_name}")
            
        # Add groups to message
        if running_groups or stopped_groups:
            if running_groups:
                status_msg += "\n".join(running_groups)
            if stopped_groups:
                if running_groups:
                    status_msg += "\n"
                status_msg += "\n".join(stopped_groups)
        else:
            status_msg += "❌ No groups found"

        await update.message.reply_text(
            status_msg,
            parse_mode='Markdown'
        )

    except Exception as e:
        logger.error(f"Status command failed - {str(e)}")
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
        CommandHandler("status", status),
        CommandHandler("startall", startall),
        CommandHandler("stopall", stopall)
        ]
