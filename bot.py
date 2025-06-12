import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes

# This function will get the Mini App URL from an environment variable we set on Render
def get_webapp_url():
    return os.environ.get("WEBAPP_URL", "https://default.url.com")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        # The URL is now dynamically fetched
        [InlineKeyboardButton("🏆 Open Tournament App 🏆", web_app={'url': get_webapp_url()})]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Welcome! Click the button below to open the tournament app.",
        reply_markup=reply_markup
    )

def main() -> None:
    """Start the bot."""
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_BOT_TOKEN:
        print("ERROR: TELEGRAM_BOT_TOKEN environment variable not set!")
        return

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))

    # Add your other bot handlers here

    print("Bot is polling for updates...")
    application.run_polling()

if __name__ == "__main__":
    main()

