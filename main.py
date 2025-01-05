import logging
from telegram import Update, Chat, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters
)
import asyncio
import json
import os
from telegram.error import TelegramError, BadRequest

# Enhanced logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# States for conversation
WAITING_FOR_MESSAGE = 1
WAITING_FOR_DELAY = 1

[Previous BotConfig and ChatManager classes remain exactly the same]

async def delete_message_with_delay(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = 3):
    """Delete a message after a specified delay."""
    try:
        await asyncio.sleep(delay)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.error(f"Error deleting message {message_id} in chat {chat_id}: {e}")

[Previous delete_previous_message and send_periodic_message functions remain exactly the same]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command and deep linking"""
    chat = update.effective_chat
    logger.info(f"Received /start command in chat {chat.id} (type: {chat.type})")
    
    if chat.type != Chat.PRIVATE:
        await start_loop(update, context)
        # Delete the /start command message
        try:
            await update.message.delete()
        except Exception as e:
            logger.error(f"Error deleting start command: {e}")
    else:
        msg = await update.message.reply_text("Add me to a group and use /startloop to begin!")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
        try:
            await update.message.delete()
        except Exception as e:
            logger.error(f"Error deleting start command: {e}")

async def start_loop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /startloop command"""
    chat = update.effective_chat
    logger.info(f"Attempting to start loop in chat {chat.id} (type: {chat.type})")

    if chat.type == Chat.PRIVATE:
        msg = await update.message.reply_text("This command can only be used in groups!")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
        return

    if chat_manager.is_active(chat.id):
        msg = await update.message.reply_text("I am already getting filled 💦🥵")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
        return

    try:
        chat_manager.add_chat(chat.id, chat.type)
        chat_manager.add_chat_info(
            chat.id,
            title=chat.title or "Unknown Group"
        )
        asyncio.create_task(send_periodic_message(context, chat.id))
        
        # Send and delete the success message
        msg = await update.message.reply_text("Loop started successfully!")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
        
        # Delete the command message
        try:
            await update.message.delete()
        except Exception as e:
            logger.error(f"Error deleting command message: {e}")
            
    except Exception as e:
        logger.error(f"Error starting loop in chat {chat.id}: {e}")
        msg = await update.message.reply_text("Failed to start the loop. Please try again.")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))

async def stop_loop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat_manager.is_active(chat.id):
        msg = await update.message.reply_text("I am free rightnow 🤫")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
        return
    
    chat_manager.chats[str(chat.id)]['error_remove'] = False
    chat_manager.remove_chat(chat.id)
    msg = await update.message.reply_text("I am going to take a nap🥱")
    asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
    
    try:
        await update.message.delete()
    except Exception as e:
        logger.error(f"Error deleting stop command: {e}")

[Previous set_message, receive_message, set_delay, receive_delay functions remain exactly the same]

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        msg = await update.message.reply_text("Admin only command!")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
        return

    status_message = f"Current delay: {config.delay}s\n\nGroup Chats:\n"
    group_chats = []

    for chat_id, chat_info in chat_manager.chats.items():
        title = chat_info.get('title', 'Unknown Group')
        active = chat_info.get('active', False)

        if not chat_info.get('error_remove', False):
            chat_status = "🟢" if active else "🔴"
            chat_text = f"{chat_status} {title}"
            group_chats.append(chat_text)

    if group_chats:
        status_message += "\n".join(group_chats)
    else:
        status_message += "No groups"

    await update.message.reply_text(status_message)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("Operation cancelled")
    asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
    return ConversationHandler.END

async def remove_commands(application):
    """Remove command suggestions from the bot"""
    try:
        await application.bot.delete_my_commands()
        logger.info("Successfully removed command suggestions")
    except Exception as e:
        logger.error(f"Error removing command suggestions: {e}")

def main():
    bot_token = "7863131684:AAEfObiOM_HS9bsFbiODvKhK67ChI7Yp99A"
    app = ApplicationBuilder().token(bot_token).build()

    # Remove command suggestions
    asyncio.create_task(remove_commands(app))

    # Message setting conversation
    msg_handler = ConversationHandler(
        entry_points=[CommandHandler('setmsg', set_message)],
        states={
            WAITING_FOR_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_message)]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    # Delay setting conversation
    delay_handler = ConversationHandler(
        entry_points=[CommandHandler('setdelay', set_delay)],
        states={
            WAITING_FOR_DELAY: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_delay)]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    # Add handlers
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('startloop', start_loop))
    app.add_handler(CommandHandler('stoploop', stop_loop))
    app.add_handler(CommandHandler('status', status))
    app.add_handler(msg_handler)
    app.add_handler(delay_handler)

    logger.info("Bot started...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
