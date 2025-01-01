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
from telegram.error import TelegramError

# Basic logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# States for conversation
WAITING_FOR_MESSAGE = 1
WAITING_FOR_DELAY = 1

class BotConfig:
    def __init__(self):
        self.config_file = 'bot_config.json'
        self.default_config = {
            'message': "https://t.me/translationx0x0xbot\n\n🔞Language_NoLimit🔞",
            'delay': 60,
            'admin_id': 5250831809  # Replace with your ID
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
        self.data_file = 'active_chats.json'
        self.chats = {}
        self.load_chats()

    def load_chats(self):
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    self.chats = json.load(f)
        except Exception as e:
            logging.error(f"Chat data load error: {e}")
            self.chats = {}

    def save_chats(self):
        try:
            with open(self.data_file, 'w') as f:
                json.dump(self.chats, f)
        except Exception as e:
            logging.error(f"Chat data save error: {e}")

    def add_chat(self, chat_id: int, chat_type: str):
        if chat_type != Chat.PRIVATE:
            self.chats[str(chat_id)] = {
                'type': chat_type,
                'failed_attempts': 0,
                'active': True,
                'title': None
            }
            self.save_chats()

    def add_chat_info(self, chat_id: int, title: str = None):
        if str(chat_id) in self.chats:
            self.chats[str(chat_id)]['title'] = title or "Unknown Group"
            self.save_chats()

    def remove_chat(self, chat_id: int):
        chat_id_str = str(chat_id)
        if chat_id_str in self.chats:
            if self.chats[chat_id_str].get('error_remove', False):
                # Complete removal if error-based
                self.chats.pop(chat_id_str, None)
            else:
                # Just mark inactive if manual stop
                self.chats[chat_id_str]['active'] = False
            self.save_chats()

    def is_active(self, chat_id: int) -> bool:
        chat = self.chats.get(str(chat_id))
        return chat is not None and chat['active']

    def update_failures(self, chat_id: int, success: bool) -> bool:
        chat_id_str = str(chat_id)
        if chat_id_str not in self.chats:
            return False

        if success:
            self.chats[chat_id_str]['failed_attempts'] = 0
        else:
            self.chats[chat_id_str]['failed_attempts'] += 1
            if self.chats[chat_id_str]['failed_attempts'] >= 3:
                self.chats[chat_id_str]['error_remove'] = True
                self.remove_chat(chat_id)
                return False
        self.save_chats()
        return True

# Initialize global instances
config = BotConfig()
chat_manager = ChatManager()

def is_admin(user_id: int) -> bool:
    return user_id == config.admin_id

async def send_periodic_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    while chat_manager.is_active(chat_id):
        try:
            await context.bot.send_message(chat_id=chat_id, text=config.message)
            chat_manager.update_failures(chat_id, True)
            await asyncio.sleep(config.delay)
        except TelegramError as e:
            logging.error(f"Telegram error in chat {chat_id}: {e}")
            if not chat_manager.update_failures(chat_id, False):
                break
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            break

async def start_loop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    
    if chat.type == Chat.PRIVATE:
        asyncio.create_task(send_periodic_message(context, chat.id))
        return
    
    if chat_manager.is_active(chat.id):
        await update.message.reply_text("Message loop already running!")
        return
    
    chat_manager.add_chat(chat.id, chat.type)
    chat_manager.add_chat_info(
        chat.id,
        title=chat.title or "Unknown Group"
    )
    asyncio.create_task(send_periodic_message(context, chat.id))

async def stop_loop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat_manager.is_active(chat.id):
        await update.message.reply_text("No active message loop!")
        return
    chat_manager.chats[str(chat.id)]['error_remove'] = False  # Ensure it's a manual stop
    chat_manager.remove_chat(chat.id)
    await update.message.reply_text("Stopped message loop")

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
        
        # Only show if not error-removed
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
    await update.message.reply_text("Operation cancelled")
    return ConversationHandler.END

def main():
    bot_token = "7671818493:AAFradIXqNYcx7IXwV2dtpK94d4nxzYKVh0"
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
    app.add_handler(msg_handler)
    app.add_handler(delay_handler)
    app.add_handler(CommandHandler('startloop', start_loop))
    app.add_handler(CommandHandler('stoploop', stop_loop))
    app.add_handler(CommandHandler('status', status))
    
    print("Bot started...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
