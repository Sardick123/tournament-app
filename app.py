import os
import threading
from flask import Flask, render_template
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

def get_webapp_url():
    return os.environ.get("WEBAPP_URL", "https://your-app-name.onrender.com")

# The /start command handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [InlineKeyboardButton("🏆 Open Tournament App 🏆", web_app={'url': get_webapp_url()})]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Welcome! Click the button below to open the tournament app.",
        reply_markup=reply_markup
    )

# Function to run the bot's polling logic
def run_bot():
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_BOT_TOKEN:
        print("ERROR: TELEGRAM_BOT_TOKEN environment variable not set!")
        return

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    # Add your other bot handlers here if you have any

    print("Bot is starting to poll for updates...")
    application.run_polling()
    print("Bot has stopped.")
    
# This ensures the bot starts running as soon as the web app starts.
print("Starting bot thread...")
bot_thread = threading.Thread(target=run_bot)
bot_thread.daemon = True
bot_thread.start()
print("Bot thread has been started.")

# The 'if __name__ == "__main__"' block is no longer needed
# because gunicorn will run the 'app' object directly.
