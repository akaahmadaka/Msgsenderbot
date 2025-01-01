import logging
from telegram import Update, Chat
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import asyncio
from datetime import datetime
import json
import os
from telegram.error import TelegramError

# Configure logging (without file logging)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Configurable variables
MESSAGE_TO_SEND = ""
INTERVAL_SECONDS = 60
DATA_FILE = "chat_data.json"
MAX_RETRY_ATTEMPTS = 3
ADMIN_ID = 5250831809  # Replace with your Telegram user ID

class ChatData:
    def __init__(self):
        self.data = {
            "private_chats": {},
            "group_chats": {}
        }
        self.load_data()

    def load_data(self):
        """Load chat data from file"""
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, 'r', encoding='utf-8') as f:
                    self.data = json.load(f)
                logging.info(f"Loaded data: {len(self.data['private_chats'])} private chats, "
                           f"{len(self.data['group_chats'])} group chats")
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            self.data = {"private_chats": {}, "group_chats": {}}

    def save_data(self):
        """Save chat data to file"""
        try:
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            logging.info("Chat data saved successfully")
        except Exception as e:
            logging.error(f"Error saving data: {e}")

    def add_chat(self, chat: Chat):
        """Add a new chat with detailed information"""
        chat_info = {
            "chat_id": chat.id,
            "title": chat.title if chat.type != Chat.PRIVATE else None,
            "username": chat.username,
            "type": chat.type,
            "active": True,
            "failed_attempts": 0,
            "added_date": datetime.utcnow().isoformat()
        }

        if chat.type == Chat.PRIVATE:
            self.data["private_chats"][str(chat.id)] = chat_info
        else:
            self.data["group_chats"][str(chat.id)] = chat_info
        
        self.save_data()

    def remove_chat(self, chat_id: int, chat_type: str):
        """Remove a chat from storage"""
        chat_id_str = str(chat_id)
        if chat_type == Chat.PRIVATE:
            self.data["private_chats"].pop(chat_id_str, None)
        else:
            self.data["group_chats"].pop(chat_id_str, None)
        self.save_data()

    def is_chat_active(self, chat_id: int) -> bool:
        """Check if a chat is active in either category"""
        chat_id_str = str(chat_id)
        if chat_id_str in self.data["private_chats"]:
            return self.data["private_chats"][chat_id_str]["active"]
        if chat_id_str in self.data["group_chats"]:
            return self.data["group_chats"][chat_id_str]["active"]
        return False

    def update_chat_status(self, chat_id: int, success: bool):
        """Update chat status after message attempt"""
        chat_id_str = str(chat_id)
        chat_data = None
        chat_type = None

        if chat_id_str in self.data["private_chats"]:
            chat_data = self.data["private_chats"][chat_id_str]
            chat_type = "private"
        elif chat_id_str in self.data["group_chats"]:
            chat_data = self.data["group_chats"][chat_id_str]
            chat_type = "group"

        if chat_data:
            if success:
                chat_data["failed_attempts"] = 0
            else:
                chat_data["failed_attempts"] += 1
                if chat_data["failed_attempts"] >= MAX_RETRY_ATTEMPTS:
                    self.remove_chat(chat_id, chat_type)
                    return False
            self.save_data()
            return True
        return False

# Initialize chat data manager
chat_data = ChatData()

async def send_periodic_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Send periodic messages with enhanced error handling"""
    global MESSAGE_TO_SEND, INTERVAL_SECONDS
    
    while chat_data.is_chat_active(chat_id):
        try:
            current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            message = f"{MESSAGE_TO_SEND}\nTime: {current_time}"
            
            await context.bot.send_message(chat_id=chat_id, text=message)
            chat_data.update_chat_status(chat_id, True)
            await asyncio.sleep(INTERVAL_SECONDS)
            
        except TelegramError as te:
            logging.error(f"Telegram error for chat {chat_id}: {te}")
            if not chat_data.update_chat_status(chat_id, False):
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="⚠️ Stopping periodic messages due to multiple failed attempts."
                    )
                except:
                    logging.error(f"Could not send final message to chat {chat_id}")
                break
        except Exception as e:
            logging.error(f"Unexpected error in send_periodic_message: {e}")
            break

def is_admin(user_id: int) -> bool:
    """Check if the user is an admin"""
    return user_id == ADMIN_ID

async def set_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to set new message"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ This command is only available to admins!")
        return

    if not context.args:
        await update.message.reply_text("❌ Please provide a message!")
        return

    global MESSAGE_TO_SEND
    MESSAGE_TO_SEND = ' '.join(context.args)
    await update.message.reply_text(f"✅ Message updated to:\n{MESSAGE_TO_SEND}")

async def set_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to set new delay"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ This command is only available to admins!")
        return

    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Please provide a valid number of seconds!")
        return

    global INTERVAL_SECONDS
    INTERVAL_SECONDS = int(context.args[0])
    await update.message.reply_text(f"✅ Delay updated to {INTERVAL_SECONDS} seconds")

async def start_loop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the periodic message loop"""
    chat = update.effective_chat
    
    if chat_data.is_chat_active(chat.id):
        await update.message.reply_text("Message loop is already running!")
        return
    
    chat_data.add_chat(chat)
    await update.message.reply_text(
        f"✅ Starting message loop!\n"
        f"⏱️ Interval: {INTERVAL_SECONDS} seconds\n"
        f"📝 Message: {MESSAGE_TO_SEND}"
    )
    
    asyncio.create_task(send_periodic_message(context, chat.id))

async def stop_loop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop the periodic message loop"""
    chat = update.effective_chat
    
    if not chat_data.is_chat_active(chat.id):
        await update.message.reply_text("No message loop is currently running!")
        return
    
    chat_data.remove_chat(chat.id, chat.type)
    await update.message.reply_text("✅ Message loop stopped!")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot status and statistics"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ This command is only available to admins!")
        return

    private_count = len(chat_data.data["private_chats"])
    group_count = len(chat_data.data["group_chats"])
    
    await update.message.reply_text(
        f"📊 Bot Status:\n"
        f"🔹 Active private chats: {private_count}\n"
        f"🔹 Active group chats: {group_count}\n"
        f"⏱️ Message interval: {INTERVAL_SECONDS}s\n"
        f"📝 Current message:\n{MESSAGE_TO_SEND}"
    )

def main():
    """Main function to run the bot"""
    bot_token = "7671818493:AAFradIXqNYcx7IXwV2dtpK94d4nxzYKVh0"
    
    application = ApplicationBuilder().token(bot_token).build()
    
    # Add command handlers
    application.add_handler(CommandHandler('startloop', start_loop))
    application.add_handler(CommandHandler('stoploop', stop_loop))
    application.add_handler(CommandHandler('status', status))
    application.add_handler(CommandHandler('setmsg', set_message))
    application.add_handler(CommandHandler('setdelay', set_delay))
    
    print("Bot is running...")
    application.run_polling()

if __name__ == '__main__':
    main()
