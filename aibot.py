import asyncio
import logging
import os
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from pyppeteer import launch

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "7671818493:AAFradIXqNYcx7IXwV2dtpK94d4nxzYKVh0")

class PuterAI:
    def __init__(self):
        self.browser = None
        self.page = None

    async def get_browser(self):
        if not self.browser or self.browser.isClosed():
            self.browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
            self.page = await self.browser.newPage()
            await self.page.setContent('''
                <html><body><script src="https://js.puter.com/v2/"></script></body></html>
            ''')
            await self.page.waitForFunction('typeof puter !== "undefined"')
        return self.page

    async def get_response(self, message: str) -> str:
        try:
            page = await self.get_browser()
            response = await page.evaluate(f'''
                async () => {{
                    try {{
                        return await puter.ai.chat(`{message}`);
                    }} catch (error) {{
                        return "Error: " + error.message;
                    }}
                }}
            ''')
            return response
        except Exception as e:
            logger.error(f"Error getting AI response: {e}")
            return "Sorry, I encountered an error processing your request."

    async def close(self):
        if self.browser and not self.browser.isClosed():
            await self.browser.close()

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Hello! I'm your AI assistant. Send me a message and I'll respond!"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Send me any message and I'll respond using AI!\n"
        "Available commands:\n"
        "/start - Start the bot\n"
        "/help - Show this help message"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Initialize PuterAI if it doesn't exist in bot_data
    if 'puter_ai' not in context.bot_data:
        context.bot_data['puter_ai'] = PuterAI()

    try:
        await context.bot.send_chat_action(
            chat_id=update.effective_chat.id,
            action="typing"
        )
        
        # Get the response from PuterAI
        response = await context.bot_data['puter_ai'].get_response(update.message.text)
        await update.message.reply_text(response)
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text("Sorry, something went wrong. Please try again later.")

async def on_shutdown(application):
    """Cleanup resources on shutdown."""
    if 'puter_ai' in application.bot_data:
        await application.bot_data['puter_ai'].close()
    logger.info("Bot shutdown complete.")

def main():
    """Start the bot."""
    # Create the Application
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Register shutdown handler
    application.add_handler(MessageHandler(filters.ALL, on_shutdown))

    # Run the bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
