# handlers.py
from telegram import Update
from telegram.ext import ContextTypes
from utils import load_data, save_data, add_group
from scheduler import schedule_message, remove_scheduled_job

async def startloop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /startloop command."""
    if update.message.chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command only works in groups!")
        return

    group_id = str(update.message.chat_id)
    group_name = update.message.chat.title

    # Add group to data file if not already present
    group_data = add_group(group_id, group_name)

    # Start the message loop
    if not group_data["loop_running"]:
        group_data["loop_running"] = True
        save_data({"groups": {group_id: group_data}})
        await schedule_message(context.bot, group_id)
        await update.message.reply_text("Message loop started!")
    else:
        await update.message.reply_text("Message loop is already running!")

async def stoploop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stoploop command."""
    if update.message.chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command only works in groups!")
        return

    group_id = str(update.message.chat_id)

    # Stop the message loop
    data = load_data()
    if group_id in data["groups"] and data["groups"][group_id]["loop_running"]:
        data["groups"][group_id]["loop_running"] = False
        save_data(data)
        remove_scheduled_job(group_id)
        await update.message.reply_text("Message loop stopped!")
    else:
        await update.message.reply_text("No active loop to stop.")

async def setmsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setmsg command (private chat only)."""
    if update.message.chat.type != "private":
        await update.message.reply_text("This command is only available in private chat.")
        return

    if not context.args:
        await update.message.reply_text("Please provide a message.")
        return

    new_message = " ".join(context.args)

    # Update message for all groups
    data = load_data()
    for group_id in data["groups"]:
        data["groups"][group_id]["message"] = new_message
        if data["groups"][group_id]["loop_running"]:
            # Reschedule with new message if loop is running
            await schedule_message(context.bot, group_id, message=new_message)
    
    save_data(data)
    await update.message.reply_text("Message updated for all groups!")

async def setdelay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setdelay command (private chat only)."""
    if update.message.chat.type != "private":
        await update.message.reply_text("This command is only available in private chat.")
        return

    try:
        new_delay = int(context.args[0])
        if new_delay < 60:  # Minimum delay of 60 seconds
            raise ValueError("Delay too short")
    except (IndexError, ValueError):
        await update.message.reply_text("Please provide a valid delay in seconds (minimum 60).")
        return

    # Update delay for all groups
    data = load_data()
    for group_id in data["groups"]:
        data["groups"][group_id]["delay"] = new_delay
        if data["groups"][group_id]["loop_running"]:
            # Reschedule with new delay if loop is running
            await schedule_message(context.bot, group_id, delay=new_delay)
    
    save_data(data)
    await update.message.reply_text(f"Delay updated to {new_delay} seconds for all groups!")