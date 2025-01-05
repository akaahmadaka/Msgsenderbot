import logging
from telegram import Update, Chat
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
from telegram.error import (
    TelegramError, BadRequest, RetryAfter
)

# Enhanced logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# States for conversation
WAITING_FOR_MESSAGE = 1
WAITING_FOR_DELAY = 2

class BotConfig:
    def __init__(self):
        self.config_file = os.environ.get('BOT_CONFIG_FILE', 'bot_config.json')
        self.default_config = {
            'message': "",
            'delay': 10,
            'admin_id': int(os.environ.get('ADMIN_ID', 35764))  # Replace with your ID
        }
        self.load_config()

    def load_config(self):
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
            else:
                self.config = self.default_config
                self.save_config()
        except Exception as e:
            logging.error(f"Config load error: {e}")
            self.config = self.default_config

    def save_config(self):
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logging.error(f"Config save error: {e}")

    @property
    def message(self):
        return self.config.get('message', self.default_config['message'])

    @message.setter
    def message(self, value):
        self.config['message'] = value
        self.save_config()

    @property
    def delay(self):
        return self.config.get('delay', self.default_config['delay'])

    @delay.setter
    def delay(self, value):
        self.config['delay'] = value
        self.save_config()

    @property
    def admin_id(self):
        return self.config.get('admin_id', self.default_config['admin_id'])

class ChatManager:
    def __init__(self):
        self.data_file = os.environ.get('CHAT_DATA_FILE', 'active_chats.json')
        self.chats = {}
        self.last_messages = {}  # Store last message IDs
        self.load_chats()

    def load_chats(self):
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    self.chats = data.get('chats', {})
                    self.last_messages = data.get('last_messages', {})
        except Exception as e:
            logger.error(f"Chat data load error: {e}")
            self.chats = {}
            self.last_messages = {}

    def save_chats(self):
        try:
            data = {
                'chats': self.chats,
                'last_messages': self.last_messages
            }
            with open(self.data_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Chat data save error: {e}")

    def add_chat(self, chat_id: int, chat_type: str):
        chat_id_str = str(chat_id)
        if chat_id_str not in self.chats and chat_type != Chat.PRIVATE:
            self.chats[chat_id_str] = {
                'type': chat_type,
                'failed_attempts': 0,
                'active': True,
                'title': None,
                'task': None  # To store the task reference
            }
            self.save_chats()

    def add_chat_info(self, chat_id: int, title: str = None):
        chat_id_str = str(chat_id)
        if chat_id_str in self.chats:
            self.chats[chat_id_str]['title'] = title or "Unknown Group"
            self.save_chats()

    def remove_chat(self, chat_id: int):
        chat_id_str = str(chat_id)
        if chat_id_str in self.chats:
            task = self.chats[chat_id_str].get('task')
            if task:
                task.cancel()
            del self.chats[chat_id_str]
            self.save_chats()

    def is_active(self, chat_id: int) -> bool:
        chat = self.chats.get(str(chat_id))
        return chat is not None and chat['active']

    def update_last_message(self, chat_id: int, message_id: int):
        self.last_messages[str(chat_id)] = message_id
        self.save_chats()

    def get_last_message(self, chat_id: int) -> int:
        return self.last_messages.get(str(chat_id))

# Initialize global instances
config = BotConfig()
chat_manager = ChatManager()

def is_admin(user_id: int) -> bool:
    """Check if the user is an admin."""
    return user_id == config.admin_id

async def delete_previous_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Attempt to delete the previous message in the chat."""
    try:
        last_message_id = chat_manager.get_last_message(chat_id)
        if last_message_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_message_id)
            except BadRequest as e:
                if "Message to delete not found" in str(e):
                    logger.info(f"Message {last_message_id} already deleted in chat {chat_id}")
                else:
                    logger.warning(f"Failed to delete message {last_message_id} in chat {chat_id}: {e}")
    except Exception as e:
        logger.error(f"Error in delete_previous_message for chat {chat_id}: {e}")

async def send_periodic_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Send periodic messages with improved error handling and message management."""
    consecutive_failures = 0
    max_consecutive_failures = 5
    base_retry_delay = 5  # Start with 5 seconds

    while chat_manager.is_active(chat_id):
        try:
            # Delete previous message
            await delete_previous_message(context, chat_id)

            # Send new message
            new_message = await context.bot.send_message(chat_id=chat_id, text=config.message)
            chat_manager.update_last_message(chat_id, new_message.message_id)
            
            # Reset failure counter on success
            consecutive_failures = 0
            
            # Wait for configured delay
            await asyncio.sleep(config.delay)

        except TelegramError as e:
            consecutive_failures += 1
            logger.error(f"Telegram error in chat {chat_id}: {e}")

            # Check if the bot is blocked, restricted, or the chat is not found
            if (
                "bot was blocked" in str(e).lower()
                or "chat not found" in str(e).lower()
                or "bot was kicked" in str(e).lower()
                or "bot was deleted" in str(e).lower()
                or "bot was restricted" in str(e).lower()
            ):
                logger.error(f"Bot was blocked, restricted, or chat not found in {chat_id}. Removing chat details.")
                chat_manager.remove_chat(chat_id)
                break

            if consecutive_failures >= max_consecutive_failures:
                logger.error(f"Too many consecutive failures in chat {chat_id}. Stopping loop.")
                chat_manager.remove_chat(chat_id)
                break

            # Exponential backoff for retry
            retry_delay = min(base_retry_delay * (2 ** consecutive_failures), 300)  # Max 5 minutes
            logger.info(f"Retrying in {retry_delay} seconds for chat {chat_id}")
            await asyncio.sleep(retry_delay)

        except Exception as e:
            logger.error(f"Unexpected error in chat {chat_id}: {e}")
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                logger.error(f"Too many consecutive failures in chat {chat_id}. Stopping loop.")
                chat_manager.remove_chat(chat_id)
                break
            await asyncio.sleep(base_retry_delay)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command and deep linking"""
    chat = update.effective_chat
    logger.info(f"Received /start command in chat {chat.id} (type: {chat.type})")
    
    if chat.type != Chat.PRIVATE:
        await start_loop(update, context)
    else:
        await update.message.reply_text("Add me to a group and use /startloop to begin!")

async def start_loop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /startloop command"""
    chat = update.effective_chat
    logger.info(f"Attempting to start loop in chat {chat.id} (type: {chat.type})")

    if chat.type == Chat.PRIVATE:
        await update.message.reply_text("This command can only be used in groups!")
        return

    if chat_manager.is_active(chat.id):
        # Remove the "I am already getting filled 💦🥵" message
        return

    try:
        chat_manager.add_chat(chat.id, chat.type)
        chat_manager.add_chat_info(chat.id, title=chat.title or "Unknown Group")
        task = asyncio.create_task(send_periodic_message(context, chat.id))
        chat_manager.chats[str(chat.id)]['task'] = task
        # Remove the "Loop started successfully!" message
        logger.info(f"Loop started successfully in chat {chat.id}")
    except Exception as e:
        logger.error(f"Error starting loop in chat {chat.id}: {e}")
        await update.message.reply_text("Failed to start the loop. Please try again.")

async def stop_loop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat_manager.is_active(chat.id):
        await update.message.reply_text("I am free rightnow 🥱")
        return
    chat_manager.chats[str(chat.id)]['active'] = False
    task = chat_manager.chats[str(chat.id)].get('task')
    if task:
        task.cancel()
    chat_manager.remove_chat(chat.id)
    await update.message.reply_text("I am going to take a nap 🥱")

async def set_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Admin only command!")
        return ConversationHandler.END
    await update.message.reply_text("Send new message:")
    return WAITING_FOR_MESSAGE

async def receive_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    config.message = update.message.text
    await update.message.reply_text("Message updated!")
    return ConversationHandler.END

async def set_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Admin only command!")
        return ConversationHandler.END
    await update.message.reply_text("Send new delay (seconds):")
    return WAITING_FOR_DELAY

async def receive_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_delay = int(update.message.text)
        if new_delay < 10:
            await update.message.reply_text("Minimum delay: 10s")
            return WAITING_FOR_DELAY
        config.delay = new_delay
        await update.message.reply_text(f"Delay updated to {new_delay}s")
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("Send a valid number!")
        return WAITING_FOR_DELAY

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Admin only command!")
        return

    status_message = f"Current delay: {config.delay}s\n\nGroup Chats:\n"
    group_chats = []

    for chat_id, chat_info in chat_manager.chats.items():
        title = chat_info.get('title', 'Unknown Group')
        active = chat_info.get('active', False)
        task = chat_info.get('task', None)
        task_status = "Running" if task and not task.done() else "Stopped"
        chat_status = "🟢" if active else "🔴"
        chat_text = f"{chat_status} {title} - {task_status}"
        group_chats.append(chat_text)

    if group_chats:
        status_message += "\n".join(group_chats)
    else:
        status_message += "No groups"

    await update.message.reply_text(status_message)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operation cancelled")
    return ConversationHandler.END

async def startall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id) or update.effective_chat.type != Chat.PRIVATE:
        await update.message.reply_text("Admin only command in private chat!")
        return
    chats_to_start = [chat_id for chat_id, chat_info in chat_manager.chats.items() if not chat_info['active']]
    if not chats_to_start:
        await update.message.reply_text("No chats to start.")
        return
    for chat_id in chats_to_start:
        chat_manager.chats[str(chat_id)]['active'] = True
        task = asyncio.create_task(send_periodic_message(context, int(chat_id)))
        chat_manager.chats[str(chat_id)]['task'] = task
    await update.message.reply_text(f"Started loops in {len(chats_to_start)} chats.")

async def stopall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id) or update.effective_chat.type != Chat.PRIVATE:
        await update.message.reply_text("Admin only command in private chat!")
        return
    chats_to_stop = [chat_id for chat_id, chat_info in chat_manager.chats.items() if chat_info['active']]
    if not chats_to_stop:
        await update.message.reply_text("No chats to stop.")
        return
    for chat_id in chats_to_stop:
        chat_manager.chats[str(chat_id)]['active'] = False
        task = chat_manager.chats[str(chat_id)].get('task')
        if task:
            task.cancel()
    await update.message.reply_text(f"Stopped loops in {len(chats_to_stop)} chats.")

def main():
    bot_token = os.environ.get('BOT_TOKEN', '45fu5fg4:35434565435676544455660')
    app = ApplicationBuilder().token(bot_token).build()

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
    app.add_handler(CommandHandler('startall', startall))
    app.add_handler(CommandHandler('stopall', stopall))
    app.add_handler(msg_handler)
    app.add_handler(delay_handler)

    logger.info("Bot started...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
