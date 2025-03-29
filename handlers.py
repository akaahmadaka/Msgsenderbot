from telegram import Update
from telegram.ext import ContextTypes, CommandHandler, ConversationHandler, filters
from utils import (
    load_data, add_group,
    update_group_status, remove_group,
    get_global_settings
)
from scheduler import scheduler
import logging
import asyncio
from config import (
    ADMIN_IDS, DEEP_LINK_TEMPLATE, WELCOME_MSG, GLOBAL_DELAY
)
from db import get_db_connection

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
            if scheduler.is_running(group_id):
                await update.message.reply_text("❌ Message loop is already running in this group!")
                return

            group_name = update.message.chat.title

            settings = None
            async with get_db_connection() as conn:
                await add_group(group_id, group_name)
                settings = await get_global_settings()

            if not settings:
                 logger.error(f"Failed to retrieve global settings for group {group_id}")
                 await update.message.reply_text("❌ Failed to retrieve settings to start loop")
                 return

            success = await scheduler.schedule_message(context.bot, group_id, settings.get("message_reference"), settings.get("delay", GLOBAL_DELAY))
            if not success:
                await update.message.reply_text("❌ Failed to start message loop")
                return

            await update.message.reply_text("✅ Message loop started")
        else:
            logger.info(f"Group {group_id} status updated to inactive")

            if not await scheduler.cleanup_group(context.bot, group_id, "Manual removal"):
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

        async with get_db_connection() as conn:
            await conn.execute("UPDATE GLOBAL_SETTINGS SET message_reference_chat_id = ?, message_reference_message_id = ? WHERE id = 1",
                        (message_reference["chat_id"], message_reference["message_id"]))
            await conn.commit()

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
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return

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

        async with get_db_connection() as conn:
            await conn.execute("UPDATE GLOBAL_SETTINGS SET delay = ? WHERE id = 1", (new_delay,))
            await conn.commit()

        from scheduler import scheduler
        updated_count = await scheduler.update_running_tasks(context.bot, new_delay=new_delay)
        await update.message.reply_text(f"✅ Global delay updated!\nNew delay: {new_delay} seconds\nUpdated {updated_count} running tasks")

    except Exception as e:
        logger.error(f"Error in setdelay: {e}")
        await update.message.reply_text("❌ Failed to update delay")

async def startall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start message loop in all manually stopped groups (Admin only)."""
    try:
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return

        if update.message.chat.type != "private":
            await update.message.reply_text("❌ This command only works in private chat!")
            return

        data = await load_data()
        settings = data["global_settings"]
        started_count = 0

        for group_id, group in data["groups"].items():
            # Check if inactive and not in an error state (assuming error_state was replaced by retry_count logic)
            if not group.get("active", False): # Simplified check, assuming retry_count doesn't block restart
                success = await scheduler.schedule_message(
                    context.bot,
                    group_id,
                    message_reference=settings.get("message_reference"),
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
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return

        if update.message.chat.type != "private":
            await update.message.reply_text("❌ This command only works in private chat!")
            return

        data = await load_data()
        stopped_count = 0

        tasks_to_remove = []
        groups_to_stop_info = []
        for group_id, group in data["groups"].items():
            if group.get("active", False):
                tasks_to_remove.append(scheduler.cleanup_group(context.bot, group_id, "Manual removal"))
                groups_to_stop_info.append(group.get('name', group_id))

        stopped_count = 0
        if tasks_to_remove:
            results = await asyncio.gather(*tasks_to_remove, return_exceptions=True)
            for i, result in enumerate(results):
                group_info = groups_to_stop_info[i]
                if isinstance(result, Exception):
                     logger.error(f"Failed to stop loop for group {group_info}: {result}")
                else:
                     stopped_count += 1
                     logger.info(f"Stopped loop in group {group_info}")

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
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Admin only command!")
            return

        data = await load_data()

        active_count = sum(1 for group in data["groups"].values() if group["active"])
        total_count = len(data["groups"])

        status_msg = (
            "📊 Bot Status\n\n"
            f"📈 Groups: {total_count} │ Active: {active_count}\n\n"
            "Group Status:\n"
        )

        running_groups = []
        stopped_groups = []

        for group_id, group in data["groups"].items():
            group_name = group["name"].replace('_', ' ').replace('|', '-')
            if group["active"]:
                running_groups.append(f"🟢 {group_name}")
            else:
                stopped_groups.append(f"🔴 {group_name}")

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
