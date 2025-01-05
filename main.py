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

class BotConfig:
    def __init__(self, filename='config.json'):
        self.filename = filename
        self.delay = 300  # Default delay of 300 seconds (5 minutes)
        self.message = "Default message"
        self.load_config()

    def load_config(self):
        """Load configuration from file"""
        try:
            if os.path.exists(self.filename):
                with open(self.filename, 'r') as f:
                    data = json.load(f)
                    self.delay = data.get('delay', self.delay)
                    self.message = data.get('message', self.message)
        except Exception as e:
            logger.error(f"Error loading config: {e}")

    def save_config(self):
        """Save configuration to file"""
        try:
            with open(self.filename, 'w') as f:
                json.dump({
                    'delay': self.delay,
                    'message': self.message
                }, f)
        except Exception as e:
            logger.error(f"Error saving config: {e}")

class ChatManager:
    def __init__(self, filename='chats.json'):
        self.filename = filename
        self.chats = {}
        self.load_chats()

    def load_chats(self):
        """Load chats from file"""
        try:
            if os.path.exists(self.filename):
                with open(self.filename, 'r') as f:
                    self.chats = json.load(f)
        except Exception as e:
            logger.error(f"Error loading chats: {e}")

    def save_chats(self):
        """Save chats to file"""
        try:
            with open(self.filename, 'w') as f:
                json.dump(self.chats, f)
        except Exception as e:
            logger.error(f"Error saving chats: {e}")

    def add_chat(self, chat_id: int, chat_type: str):
        """Add a new chat"""
        self.chats[str(chat_id)] = {
            'type': chat_type,
            'active': True,
            'error_remove': False
        }
        self.save_chats()

    def remove_chat(self, chat_id: int):
        """Remove a chat"""
        if str(chat_id) in self.chats:
            self.chats[str(chat_id)]['active'] = False
            self.save_chats()

    def is_active(self, chat_id: int) -> bool:
        """Check if a chat is active"""
        return self.chats.get(str(chat_id), {}).get('active', False)

    def add_chat_info(self, chat_id: int, **kwargs):
        """Add additional info to a chat"""
        if str(chat_id) in self.chats:
            self.chats[str(chat_id)].update(kwargs)
            self.save_chats()

# Initialize global config and chat manager
config = BotConfig()
chat_manager = ChatManager()

# Admin IDs list
ADMIN_IDS = [5756637938]

def is_admin(user_id: int) -> bool:
    """Check if a user is an admin"""
    return user_id in ADMIN_IDS

async def delete_message_with_delay(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = 3):
    """Delete a message after a specified delay."""
    try:
        await asyncio.sleep(delay)
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.error(f"Error deleting message {message_id} in chat {chat_id}: {e}")

async def delete_previous_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Delete the previous message in a chat if it exists."""
    if 'last_message' in context.chat_data:
        try:
            await context.bot.delete_message(
                chat_id=chat_id,
                message_id=context.chat_data['last_message']
            )
        except Exception as e:
            logger.error(f"Error deleting previous message: {e}")

async def send_periodic_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Send periodic messages to a chat."""
    while chat_manager.is_active(chat_id):
        try:
            # Delete previous message if it exists
            await delete_previous_message(context, chat_id)
            
            # Send new message and store its ID
            message = await context.bot.send_message(chat_id=chat_id, text=config.message)
            context.chat_data['last_message'] = message.message_id
            
            # Wait for configured delay
            await asyncio.sleep(config.delay)
        except Exception as e:
            logger.error(f"Error in periodic message loop for chat {chat_id}: {e}")
            chat_manager.chats[str(chat_id)]['error_remove'] = True
            chat_manager.remove_chat(chat_id)
            break

async def start_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start periodic messages in all stopped groups"""
    chat = update.effective_chat
    user_id = update.effective_user.id
    
    # Check if command is used in private chat and by admin
    if chat.type != Chat.PRIVATE:
        msg = await update.message.reply_text("This command can only be used in private chat!")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
        return
        
    if not is_admin(user_id):
        msg = await update.message.reply_text("Admin only command!")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
        return

    started_count = 0
    status_message = "Starting periodic messages in all stopped groups:\n\n"

    for chat_id, chat_info in chat_manager.chats.items():
        chat_id = int(chat_id)
        if not chat_info.get('active', False) and not chat_info.get('error_remove', False):
            try:
                chat_manager.chats[str(chat_id)]['active'] = True
                asyncio.create_task(send_periodic_message(context, chat_id))
                started_count += 1
                status_message += f"✅ Started in {chat_info.get('title', 'Unknown Group')}\n"
            except Exception as e:
                logger.error(f"Error starting loop in chat {chat_id}: {e}")
                status_message += f"❌ Failed to start in {chat_info.get('title', 'Unknown Group')}\n"

    if started_count == 0:
        status_message = "No stopped groups found to start!"

    await update.message.reply_text(status_message)

async def stop_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop periodic messages in all active groups"""
    chat = update.effective_chat
    user_id = update.effective_user.id
    
    # Check if command is used in private chat and by admin
    if chat.type != Chat.PRIVATE:
        msg = await update.message.reply_text("This command can only be used in private chat!")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
        return
        
    if not is_admin(user_id):
        msg = await update.message.reply_text("Admin only command!")
        asyncio.create_task(delete_message_with_delay(context, chat.id, msg.message_id))
        return

    stopped_count = 0
    status_message = "Stopping periodic messages in all active groups:\n\n"

    for chat_id, chat_info in chat_manager.chats.items():
        chat_id = int(chat_id)
        if chat_info.get('active', False) and not chat_info.get('error_remove', False):
            try:
                chat_manager.chats[str(chat_id)]['active'] = False
                chat_manager.chats[str(chat_id)]['error_remove'] = False
                stopped_count += 1
                status_message += f"✅ Stopped in {chat_info.get('title', 'Unknown Group')}\n"
            except Exception as e:
                logger.error(f"Error stopping loop in chat {chat_id}: {e}")
                status_message += f"❌ Failed to stop in {chat_info.get('title', 'Unknown Group')}\n"

    if stopped_count == 0:
        status_message = "No active groups found to stop!"

    await update.message.reply_text(status_message)

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

async def set_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the message setting conversation."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        msg = await update.message.reply_text("Admin only command!")
        asyncio.create_task(delete_message_with_delay(context, update.effective_chat.id, msg.message_id))
        return ConversationHandler.END
    
    await update.message.reply_text("Please send the new message text:")
    return WAITING_FOR_MESSAGE

async def receive_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the received message text."""
    config.message = update.message.text
    config.save_config()
    await update.message.reply_text("Message updated successfully!")
    return ConversationHandler.END

async def set_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the delay setting conversation."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        msg = await update.message.reply_text("Admin only command!")
        asyncio.create_task(delete_message_with_delay(context, update.effective_chat.id, msg.message_id))
        return ConversationHandler.END
    
    await update.message.reply_text("Please send the new delay in seconds:")
    return WAITING_FOR_DELAY

async def receive_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the received delay value."""
    try:
        new_delay = int(update.message.text)
        if new_delay < 1:
            raise ValueError("Delay must be positive")
        config.delay = new_delay
        config.save_config()
        await update.message.reply_text(f"Delay updated to {new_delay} seconds!")
    except ValueError:
        await update.message.reply_text("Please send a valid positive number!")
        return WAITING_FOR_DELAY
    return ConversationHandler.END

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        msg = await update.message.reply_text("Admin only command!")
        asyncio.create_task(delete_message_with_delay(context, update.effective_chat.id, msg.message_id))
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
    asyncio.create_task(delete_message_with_delay(context, update.effective_chat.id, msg.message_id))
    return ConversationHandler.END

async def remove_commands(application):
    """Remove command suggestions from the bot"""
    try:
        await application.bot.delete_my_commands()
        logger.info("Successfully removed command suggestions")
    except Exception as e:
        logger.error(f"Error removing command suggestions: {e}")

async def main():
    bot_token = "7671818493:AAFradIXqNYcx7IXwV2dtpK94d4nxzYKVh0"
    app = ApplicationBuilder().token(bot_token).build()

    # Remove command suggestions
    await remove_commands(app)

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
    app.add_handler(CommandHandler('startall', start_all))
    app.add_handler(CommandHandler('stopall', stop_all))
    app.add_handler(CommandHandler('status', status))
    app.add_handler(msg_handler)
    app.add_handler(delay_handler)

    logger.info("Bot started...")
    await app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
