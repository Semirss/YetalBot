import os
from dotenv import load_dotenv
from pymongo import MongoClient
from telegram.ext import Updater, CommandHandler
from telegram.error import BadRequest

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["yetal"]
collection = db["yetalcollection"]

# ======================
# /addchannel
# ======================
def add_channel(update, context):
    if len(context.args) == 0:
        update.message.reply_text("⚡ Usage: /addchannel @ChannelUsername")
        return

    username = context.args[0].strip()

    if not username.startswith("@"):
        update.message.reply_text("❌ Please provide a valid channel username starting with @")
        return

    # Check if already in DB
    if collection.find_one({"username": username}):
        update.message.reply_text("⚠️ This channel is already saved in the database.")
        return

    try:
        chat = context.bot.get_chat(username)  # Validate channel
        # Save both username + title for later
        collection.insert_one({"username": username, "title": chat.title})
        update.message.reply_text(
            f"✅ <b>Channel saved successfully!</b>\n\n"
            f"📌 <b>Name:</b> {chat.title}\n"
            f"🔗 <b>Username:</b> {username}",
            parse_mode="HTML"
        )

    except BadRequest as e:
        update.message.reply_text(f"❌ Could not add channel:\n<code>{str(e)}</code>", parse_mode="HTML")

# ======================
# /listchannels
# ======================
def list_channels(update, context):
    channels = list(collection.find({}))
    if not channels:
        update.message.reply_text("📭 No channels saved yet.")
        return

    msg_lines = ["📃 <b>Saved Channels:</b>\n"]
    for ch in channels:
        username = ch.get("username")
        if not username:
            continue

        try:
            chat = context.bot.get_chat(username)
            status = "✅"
            title = chat.title
            # Update DB with latest title
            collection.update_one({"username": username}, {"$set": {"title": title}})
        except BadRequest:
            status = "❌"
            title = ch.get("title", "Unknown")

        msg_lines.append(f"{status} {username} — <b>{title}</b>")

    msg = "\n".join(msg_lines)

    # Split if too long (Telegram limit = 4096 chars)
    for chunk in [msg[i:i+4000] for i in range(0, len(msg), 4000)]:
        update.message.reply_text(chunk, parse_mode="HTML")

# ======================
# /checkchannel
# ======================
def check_channel(update, context):
    if len(context.args) == 0:
        update.message.reply_text("⚡ Usage: /checkchannel @ChannelUsername")
        return

    username = context.args[0].strip()

    if not username.startswith("@"):
        update.message.reply_text("❌ Please provide a valid channel username starting with @")
        return

    try:
        chat = context.bot.get_chat(username)
        update.message.reply_text(
            f"🔍 <b>Channel check result:</b>\n\n"
            f"✅ <b>Exists!</b>\n"
            f"📌 <b>Name:</b> {chat.title}\n"
            f"🔗 <b>Username:</b> {username}",
            parse_mode="HTML"
        )
        # Update DB title if saved
        collection.update_one({"username": username}, {"$set": {"title": chat.title}}, upsert=True)
    except BadRequest as e:
        update.message.reply_text(f"❌ Channel not found or inaccessible:\n<code>{str(e)}</code>", parse_mode="HTML")

# ======================
# Main
# ======================
def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("addchannel", add_channel))
    dp.add_handler(CommandHandler("listchannels", list_channels))
    dp.add_handler(CommandHandler("checkchannel", check_channel))

    print("🚀 Bot is running...")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
