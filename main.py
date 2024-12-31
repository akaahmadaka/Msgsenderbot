import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import asyncio
from datetime import datetime

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Configurable variables - you can modify these later
MESSAGE_TO_SEND = "https://t.me/translationx0x0xbot\n\n🔞Language_NoLimit🔞"  # Change this to your desired message
INTERVAL_SECONDS = 60  # Change this to your desired interval in seconds

# Dictionary to store active loops for different chats
active_loops = {}

async def send_periodic_message(context, chat_id):
    """Send periodic messages until the loop is stopped."""
    while chat_id in active_loops:
        try:
            # You can modify the message format here
            current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            message = f"{MESSAGE_TO_SEND}\nTime: {current_time}"
            
            await context.bot.send_message(chat_id=chat_id, text=message)
            await asyncio.sleep(INTERVAL_SECONDS)
        except Exception as e:
            logging.error(f"Error in send_periodic_message: {e}")
            break

async def start_loop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the periodic message loop."""
    chat_id = update.effective_chat.id
    
    if chat_id in active_loops:
        await update.message.reply_text("Message loop is already running!")
        return
    
    active_loops[chat_id] = True
    await update.message.reply_text(
        f"Starting message loop!\nInterval: {INTERVAL_SECONDS} seconds\nMessage: {MESSAGE_TO_SEND}"
    )
    
    # Start the periodic message task
    asyncio.create_task(send_periodic_message(context, chat_id))

async def stop_loop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop the periodic message loop."""
    chat_id = update.effective_chat.id
    
    if chat_id not in active_loops:
        await update.message.reply_text("No message loop is currently running!")
        return
    
    del active_loops[chat_id]
    await update.message.reply_text("Message loop stopped!")

def main():
    """Main function to run the bot."""
    # Replace 'YOUR_BOT_TOKEN' with your actual bot token from BotFather
    bot_token = "7671818493:AAFradIXqNYcx7IXwV2dtpK94d4nxzYKVh0"
    
    # Create the application
    application = ApplicationBuilder().token(bot_token).build()
    
    # Add command handlers
    application.add_handler(CommandHandler('startloop', start_loop))
    application.add_handler(CommandHandler('stoploop', stop_loop))
    
    # Start the bot
    print("Bot is running...")
    application.run_polling()

if __name__ == '__main__':
    main()
