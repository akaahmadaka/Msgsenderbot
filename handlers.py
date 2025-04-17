from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ContextTypes, CommandHandler, ConversationHandler, filters,
    CallbackQueryHandler, MessageHandler
)
from utils import (
    load_data, add_group, update_group_status, remove_group,
    get_global_settings, clear_global_messages, add_global_message,
    get_global_messages
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

# Conversation states for /setmsg
ADDING_MESSAGES, CONFIRM_MESSAGES = range(2)

def is_admin(user_id: int) -> bool:
    """Check if user is an admin."""
    return user_id in ADMIN_IDS

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command and deep linking."""
    try:
        chat_type = update.message.chat.type
        if chat_type in ["group", "supergroup"]:
            if context.args and context.args[0] == "GetVideo":
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
                # Loop already running, do nothing silently
                return

            group_name = update.message.chat.title

            settings = None
            global_messages = await get_global_messages()
            if not global_messages:
                await update.message.reply_text("❌ Cannot start loop: No global messages are set. Use /setmsg first.")
                return

            async with get_db_connection() as conn:
                await add_group(group_id, group_name)
                settings = await get_global_settings()

            if not settings:
                 logger.error(f"Failed to retrieve global settings for group {group_id}")
                 await update.message.reply_text("❌ Failed to retrieve settings to start loop")
                 return

            success = await scheduler.schedule_message(context.bot, group_id, delay=settings.get("delay", GLOBAL_DELAY))
            if not success:
                await update.message.reply_text("❌ Failed to start message loop")
                return

        else:
            logger.info(f"Group {group_id} status updated to inactive")

            if not await scheduler.cleanup_group(context.bot, group_id, "Manual removal"):
                logger.error(f"Failed to stop message loop for group {group_id}")
                await update.message.reply_text("❌ Failed to stop message loop")
                return

            logger.info(f"Message loop stopped for group {group_id}")

    except Exception as e:
        logger.error(f"Error in toggle_loop: {e}")
        await update.message.reply_text(f"❌ Failed to {'start' if start else 'stop'} message loop")


async def setmsg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Starts the /setmsg conversation (Admin only)."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Admin only command!")
        return ConversationHandler.END

    if update.message.chat.type != "private":
        await update.message.reply_text("❌ This command only works in private chat!")
        return ConversationHandler.END

    try:
        await clear_global_messages()
        context.user_data['pending_messages'] = []
        logger.info(f"Admin {user_id} started /setmsg. Cleared previous messages.")
        await update.message.reply_text(
            "🗑️ Previous global messages cleared.\n"
            "📝 Please send the *first* message you want the bot to loop."
        )
        return ADDING_MESSAGES
    except Exception as e:
        logger.error(f"Error starting setmsg for admin {user_id}: {e}")
        await update.message.reply_text("❌ An error occurred starting the process.")
        return ConversationHandler.END

async def receive_message_for_setmsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receives a message during the /setmsg flow."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        logger.warning(f"Non-admin {user_id} attempted to send message during setmsg.")
        return ConversationHandler.END

    if 'pending_messages' not in context.user_data:
        logger.warning(f"Admin {user_id} sent message but 'pending_messages' not in user_data. Restarting.")
        await update.message.reply_text("⚠️ Something went wrong, please start again with /setmsg.")
        return ConversationHandler.END

    message = update.effective_message
    message_ref = {
        "chat_id": message.chat_id,
        "message_id": message.message_id
    }
    context.user_data['pending_messages'].append(message_ref)
    msg_count = len(context.user_data['pending_messages'])

    logger.info(f"Admin {user_id} added message {msg_count} (ID: {message.message_id}) to pending list.")

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm Messages", callback_data="confirm_setmsg"),
            InlineKeyboardButton("➕ Add More", callback_data="add_more_setmsg"),
        ]
    ])

    await update.message.reply_text(
        f"✅ Message {msg_count} added (ID: {message.message_id}).\n"
        f"Total messages pending: {msg_count}\n\n"
        "Do you want to add another message or confirm the current list?",
        reply_markup=keyboard
    )
    return CONFIRM_MESSAGES

async def handle_setmsg_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles button presses during the /setmsg flow."""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    if not is_admin(user_id):
        logger.warning(f"Non-admin {user_id} pressed setmsg button.")
        await query.edit_message_text("❌ Action not allowed.")
        return ConversationHandler.END

    if 'pending_messages' not in context.user_data:
        logger.warning(f"Admin {user_id} pressed button but 'pending_messages' not in user_data.")
        await query.edit_message_text("⚠️ Something went wrong, please start again with /setmsg.")
        return ConversationHandler.END

    choice = query.data

    if choice == "add_more_setmsg":
        logger.info(f"Admin {user_id} chose to add more messages.")
        await query.edit_message_text("📝 Okay, please send the next message.")
        return ADDING_MESSAGES

    elif choice == "confirm_setmsg":
        logger.info(f"Admin {user_id} chose to confirm messages.")
        pending_messages = context.user_data.get('pending_messages', [])

        if not pending_messages:
            logger.warning(f"Admin {user_id} confirmed with no pending messages.")
            await query.edit_message_text("⚠️ No messages were added. Please start again with /setmsg.")
            context.user_data.pop('pending_messages', None)
            return ConversationHandler.END

        try:
            save_tasks = []
            for index, msg_ref in enumerate(pending_messages):
                save_tasks.append(
                    add_global_message(msg_ref['chat_id'], msg_ref['message_id'], index)
                )
            results = await asyncio.gather(*save_tasks, return_exceptions=True)

            errors = [res for res in results if isinstance(res, Exception)]
            if errors:
                logger.error(f"Failed to save some global messages for admin {user_id}: {errors}")
                await query.edit_message_text(f"❌ Failed to save all messages ({len(errors)} errors). Please try again.")
                await clear_global_messages()
            else:
                num_saved = len(pending_messages)
                logger.info(f"Admin {user_id} successfully confirmed and saved {num_saved} global messages.")
                await query.edit_message_text(
                    f"✅ Global messages updated!\n"
                    f"Total messages set: {num_saved}\n\n"
                    "Running tasks will start using the new message sequence on their next cycle."
                )

            context.user_data.pop('pending_messages', None)
            return ConversationHandler.END

        except Exception as e:
            logger.error(f"Error confirming/saving messages for admin {user_id}: {e}")
            await query.edit_message_text("❌ An error occurred while saving the messages. Please try again.")
            await clear_global_messages()
            context.user_data.pop('pending_messages', None)
            return ConversationHandler.END

async def setmsg_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancels the /setmsg conversation."""
    user_id = update.effective_user.id
    logger.info(f"Admin {user_id} cancelled /setmsg.")
    context.user_data.pop('pending_messages', None)
    await update.message.reply_text("❌ Message setup cancelled. No changes were made.")
    return ConversationHandler.END

setmsg_conversation = ConversationHandler(
    entry_points=[CommandHandler("setmsg", setmsg_start)],
    states={
        ADDING_MESSAGES: [MessageHandler(filters.FORWARDED | filters.TEXT | filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.VOICE | filters.AUDIO | filters.Document.ALL | filters.Sticker.ALL, receive_message_for_setmsg)],
        CONFIRM_MESSAGES: [CallbackQueryHandler(handle_setmsg_button)],
    },
    fallbacks=[CommandHandler("cancel", setmsg_cancel)],
    per_user=True,
    per_chat=False,
    #per_callback_query=True
)


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

        global_messages = await get_global_messages()
        if not global_messages:
            await update.message.reply_text("❌ Cannot start loops: No global messages are set. Use /setmsg first.")
            return

        data = await load_data()
        settings = data["global_settings"]
        started_count = 0

        for group_id, group in data["groups"].items():
            if not group.get("active", False):
                success = await scheduler.schedule_message(
                    context.bot,
                    group_id,
                    delay=settings.get("delay", GLOBAL_DELAY)
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
