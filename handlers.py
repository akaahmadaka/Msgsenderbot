# handlers.py
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler, ConversationHandler, filters
from utils import (
    load_data, add_group,
    save_data, update_group_status, remove_group,
    get_global_settings
)
from scheduler import (
    schedule_message, remove_scheduled_job, 
    is_running, get_active_tasks_count
)
import logging
from config import (
    ADMIN_IDS, DEEP_LINK_TEMPLATE, WELCOME_MSG, GLOBAL_DELAY
)
from db import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

WAITING_FOR_MESSAGE = 1

def is_admin(user_id: int) -> bool:
    """Check if user is an admin."""
    return user_id in ADMIN_IDS

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command and deep linking."""
    try:
        chat_type = update.message.chat.type
        if chat_type in ["group", "supergroup"]:
            if context.args and context.args[0] == "startloop":
                await toggle_loop(update, context, True)
            return

        bot_username = (await context.bot.get_me()).username
        deep_link = DEEP_LINK_TEMPLATE.format(bot_username=bot_username)
        welcome_message = WELCOME_MSG.format(deep_link=deep_link)
        await update.message.reply_text(welcome_message)

    except Exception as e:
        logger.error(f"Error in start command: {e}")
        await update.message.reply_text("❌ An error occurred")

async def toggle_loop(update: Update, context: ContextTypes.DEFAULT_TYPE, start: bool):
    """Toggle message loop in a group."""
    try:
        if update.message.chat.type not in ["group", "supergroup"]:
            await update.message.reply_text("❌ This command only works in groups!")
            return

        group_id = str(update.message.chat_id)

        if start:
            if is_running(group_id):
                await update.message.reply_text("❌ Message loop is already running in this group!")
                return

            group_name = update.message.chat.title
            
            with get_db_connection() as conn:
                cursor = conn.cursor()
                try:
                    add_group(group_id, group_name, cursor)
                    settings = get_global_settings(cursor)
                    conn.commit()
                finally:
                    conn.close()

            success = await schedule_message(context.bot, group_id, settings.get("message_reference"), GLOBAL_DELAY)
            if not success:
                await update.message.reply_text("❌ Failed to start message loop")
                return

            await update.message.reply_text("✅ Message loop started")
        else:
            data = load_data()

            if group_id not in data["groups"]:
                logger.info(f"Stop command failed - Group {group_id} not found in database")
                await update.message.reply_text("❌ Group not found in database")
                return

            update_group_status(group_id, False)
            logger.info(f"Group {group_id} status updated to inactive")

            if not await remove_scheduled_job(group_id):
                logger.error(f"Failed to stop message loop for group {group_id}")
                await update.message.reply_text("❌ Failed to stop message loop")
                return

            logger.info(f"Message loop stopped for group {group_id}")
            await update.message.reply_text("✅ Message loop stopped")

    except Exception as e:
        logger.error(f"Error in toggle_loop: {e}")
        await update.message.reply_text(f"❌ Failed to {'start' if start else 'stop'} message loop")

async def setmsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the setmsg conversation (Admin only)."""
    try:
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return ConversationHandler.END

        if update.message.chat.type != "private":
            await update.message.reply_text("❌ This command only works in private chat!")
            return ConversationHandler.END

        await update.message.reply_text("📝 Please send the new message you want to set:")
        return WAITING_FOR_MESSAGE

    except Exception as e:
        logger.error(f"Error in setmsg: {e}")
        await update.message.reply_text("❌ Failed to update message")
        return ConversationHandler.END
        
async def receive_new_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the received message for setmsg."""
    try:
        from db import get_db_connection
        
        if not is_admin(update.effective_user.id):
            return ConversationHandler.END

        message_reference = {
            "chat_id": update.effective_message.chat_id,
            "message_id": update.effective_message.message_id
        }
        
        # Update message in database
        with get_db_connection() as conn:
            cursor = conn.cursor()
            conn.execute("UPDATE GLOBAL_SETTINGS SET message_reference_chat_id = ?, message_reference_message_id = ? WHERE id = 1",
                        (message_reference["chat_id"], message_reference["message_id"]))
            conn.commit()

        from scheduler import scheduler
        updated_count = await scheduler.update_running_tasks(
            context.bot,
            new_message_reference=message_reference
        )
        
        await update.message.reply_text(f"✅ Global message updated!\nMessage ID: {message_reference['message_id']}\nUpdated {updated_count} running tasks")
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Error updating message: {e}")
        await update.message.reply_text("❌ Failed to update message")
        return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the conversation."""
    await update.message.reply_text("❌ Message update cancelled.")
    return ConversationHandler.END

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

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE GLOBAL_SETTINGS SET delay = ? WHERE id = 1", (new_delay,))
            conn.commit()

           # Update running tasks
            from scheduler import scheduler
            updated_count = await scheduler.update_running_tasks(context.bot, new_delay=new_delay)
            await update.message.reply_text(f"✅ Global delay updated!\nNew delay: {new_delay} seconds\nUpdated {updated_count} running tasks")

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
        settings = data["global_settings"]
        started_count = 0
        
        # Get list of manually stopped groups
        for group_id, group in data["groups"].items():
            if not group.get("active", False) and not group.get("error_state", False):
                success = await schedule_message(
                    context.bot,
                    group_id,
                    message_reference=settings.get("message_reference"),  # Use message_reference
                    delay=GLOBAL_DELAY
                )
                if success:
                    started_count += 1
                    logger.info(f"Restarted loop in group {group['name']}")

        if started_count > 0:
            await update.message.reply_text(f"✅ Successfully started message loop in {started_count} groups!")
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
            await update.message.reply_text(f"✅ Successfully stopped message loop in {stopped_count} groups!")
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

        with get_db_connection() as conn:
            cursor = conn.cursor()
            data = load_data(cursor)
        
        # Count groups
        active_count = sum(1 for group in data["groups"].values() if group.get("active", False))
        total_count = len(data["groups"])

        # Function to clean group names
        # Create status message with emojis and formatting
        status_msg = (
            "📊 Bot Status\n\n"
            f"📈 Groups: {total_count} │ Active: {active_count}\n\n"
            "Group Status:\n"
        )

        # Separate active and stopped groups
        running_groups = []
        stopped_groups = []
        
        for group_id, group in data["groups"].items():
            group_name = group.get("name", "Unknown Group").replace('_', ' ').replace('|', '-')
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

        await update.message.reply_text(status_msg)

    except Exception as e:
        logger.error(f"Status command failed - {str(e)}")
        await update.message.reply_text("❌ Failed to get status")

def get_handlers():
    """Return all command handlers."""
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("setmsg", setmsg)],
        states={
            WAITING_FOR_MESSAGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_new_message)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    
    return [
        CommandHandler("start", start, filters.ChatType.PRIVATE | filters.ChatType.GROUPS),
        CommandHandler("startloop", lambda update, context: toggle_loop(update, context, True)),
        CommandHandler("stoploop", lambda update, context: toggle_loop(update, context, False)),
        conv_handler,  # Add conversation handler
        CommandHandler("setdelay", setdelay),
        CommandHandler("status", status),
        CommandHandler("startall", startall),
        CommandHandler("stopall", stopall)
    ]