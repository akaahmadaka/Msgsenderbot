import asyncpg # Import asyncpg for error handling if needed
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ContextTypes, CommandHandler, ConversationHandler, filters,
    CallbackQueryHandler, MessageHandler
)
from utils import ( # Updated imports
    load_data, add_group, update_group_status, remove_group,
    get_global_settings, clear_global_messages, add_global_message,
    get_global_messages, update_global_delay, increment_group_click_count # Added increment_group_click_count
)
from scheduler import scheduler
import logging
import asyncio
from config import ( # Keep config imports for now, but plan to move to env vars
    ADMIN_IDS, DEEP_LINK_TEMPLATE, WELCOME_MSG, GLOBAL_DELAY
)
# Removed: from db import get_db_connection

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
            if context.args and context.args[0] == "getvideo":
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
            # Removed redundant 'async with get_db_connection()' block
            # Utils functions now handle their own connections
            # Corrected indentation: These should be inside the 'if start:' block
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

    # Define the default message content and button
    DEFAULT_MSG_TEXT = """🔞⬇️ FREE VIDEOS BOT ⬇️🔞

⬇️🔽🔽🔽🔽🔽🔽🔽🔽🔽⬇️
⬇️ click on button below ⬇️ Select chat ⬆️ i will start sending videos ⬇️"""
    DEFAULT_MSG_BUTTON_TEXT = "Get Videos"
    DEFAULT_MSG_BUTTON_URL = "http://t.me/{bot_username}?startgroup=getvideo" # Needs bot username later

    try:
        await clear_global_messages()
        context.user_data['pending_messages'] = [] # Initialize pending messages list

        logger.info(f"Admin {user_id} started /setmsg. Cleared previous messages.")

        # Send the default message with button to the admin's private chat to get its reference
        bot_username = (await context.bot.get_me()).username
        button_url = DEFAULT_MSG_BUTTON_URL.format(bot_username=bot_username)
        # IMPORTANT: For saving, we don't use the URL button. The button is added dynamically by the scheduler.
        # We just need to save the text message reference.
        default_msg_sent = await update.message.reply_text(DEFAULT_MSG_TEXT)

        # Store the reference for the default message (index 0)
        default_message_ref = {
            "chat_id": default_msg_sent.chat_id,
            "message_id": default_msg_sent.message_id
        }
        context.user_data['pending_messages'].append(default_message_ref)
        logger.info(f"Stored default message reference (Index 0): {default_message_ref['message_id']}")

        # Ask for the NEXT message
        await update.message.reply_text(
             "✅ Default message (with 'Get Videos' button) has been set as the first message.\n"
             "📝 Please send the *next* message you want the bot to loop (this will be message #2)."
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
    # msg_count includes the default message already added
    msg_count = len(context.user_data['pending_messages'])

    logger.info(f"Admin {user_id} added message {msg_count} (actual index {msg_count-1}) (ID: {message.message_id}) to pending list.")

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm Messages", callback_data="confirm_setmsg"),
            InlineKeyboardButton("➕ Add More", callback_data="add_more_setmsg"),
        ]
    ])

    await update.message.reply_text(
        f"✅ Message #{msg_count} added (ID: {message.message_id}).\n" # Show user-facing count (1-based)
        f"Total messages to save: {msg_count} (including the default message).\n\n"
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
                    f"Total messages set: {num_saved} (including default message).\n\n"
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

        # Use the utility function to update the delay
        success = await update_global_delay(new_delay)

        if not success:
             # Error logged in util function
             await update.message.reply_text("❌ Failed to update global delay due to a database error.")
             return

        from scheduler import scheduler
        updated_count = await scheduler.update_running_tasks(context.bot, new_delay=new_delay)
        await update.message.reply_text(
            f"✅ Global delay updated successfully!\n"
            f"New delay: {new_delay} seconds.\n"
            f"Attempted to update {updated_count} running group loops with the new delay."
        )

    except Exception as e:
        logger.error(f"Error in setdelay command: {e}", exc_info=True)
        await update.message.reply_text("❌ An unexpected error occurred while setting the delay.")

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

        # Prepare group lists with click counts
        group_details = []
        for group_id, group in data["groups"].items():
            group_details.append({
                "id": group_id,
                "name": group["name"].replace('_', ' ').replace('|', '-'),
                "active": group["active"],
                "click_count": group.get("click_count", 0) # Get click count, default 0
            })

        # Sort groups by click count descending
        group_details.sort(key=lambda x: x["click_count"], reverse=True)

        running_groups_str = []
        stopped_groups_str = []

        for group in group_details:
            group_name = group["name"]
            click_count = group["click_count"]
            if group["active"]:
                running_groups_str.append(f"🟢 {group_name} (Clicks: {click_count})")
            else:
                stopped_groups_str.append(f"🔴 {group_name} (Clicks: {click_count})")

        if running_groups_str or stopped_groups_str:
            if running_groups_str:
                status_msg += "\n".join(running_groups_str)
            if stopped_groups_str:
                if running_groups_str:
                    status_msg += "\n" # Add newline only if running groups were listed
                status_msg += "\n".join(stopped_groups_str)
        else:
            status_msg += "❌ No groups found"

        await update.message.reply_text(status_msg)

    except Exception as e:
        logger.error(f"Status command failed - {str(e)}")
        await update.message.reply_text("❌ Failed to get status")


async def handle_get_videos_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles clicks on the 'Get Videos' button."""
    query = update.callback_query
    try:
        # Answer the callback query immediately to remove the "loading" state
        await query.answer()

        callback_data = query.data
        # Extract group_id from callback_data (e.g., "get_videos_click_-100123456")
        prefix = "get_videos_click_"
        if callback_data.startswith(prefix):
            group_id = callback_data[len(prefix):]
            try:
                # Increment the click count
                success = await increment_group_click_count(group_id)
                if success:
                    logger.info(f"Incremented click count for group {group_id} via button click by user {query.from_user.id}")
                else:
                     logger.warning(f"Button click for non-existent group {group_id} by user {query.from_user.id}")
                # No need to reply to the user, the button click itself is the action.
            except Exception as e:
                 logger.error(f"Error incrementing click count for group {group_id} on button click: {e}")
        else:
             logger.warning(f"Received unexpected callback data: {callback_data}")

    except Exception as e:
        logger.error(f"Error handling 'Get Videos' button click: {e}")
