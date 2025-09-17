import os
import asyncio
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pymongo import MongoClient
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from telegram.error import BadRequest
from telethon import TelegramClient

# === 🔐 Load environment variables ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
API_ID = 24916488
API_HASH = '3b7788498c56da1a02e904ff8e92d494'
FORWARD_CHANNEL = os.getenv("FORWARD_CHANNEL")  # target channel username

# === ⚡ MongoDB Setup ===
client = MongoClient(MONGO_URI)
db = client["yetal"]
collection = db["yetalcollection"]

# ======================
# Forward last 24h posts from a given channel
# ======================
async def forward_last_24h(channel_username: str):
    user = TelegramClient("user_session", API_ID, API_HASH)
    await user.start()

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    messages_to_forward = []
    async for message in user.iter_messages(channel_username, limit=None):
        if message.date < cutoff:
            break
        if message.text or message.media:
            messages_to_forward.append(message)

    if messages_to_forward:
        # Maintain chronological order
        messages_to_forward.reverse()

        print(f"➡️ Forwarding {len(messages_to_forward)} messages from {channel_username}...")
        for i in range(0, len(messages_to_forward), 100):
            batch = messages_to_forward[i:i+100]
            try:
                await user.forward_messages(
                    entity=FORWARD_CHANNEL,
                    messages=[msg.id for msg in batch],
                    from_peer=channel_username
                )
            except Exception as e:
                print(f"❌ Error forwarding batch: {e}")
                await user.disconnect()
                return False, f"❌ Error forwarding: {str(e)}"
            await asyncio.sleep(1)

        await user.disconnect()
        return True, f"✅ Forwarded {len(messages_to_forward)} posts from {channel_username}."
    else:
        await user.disconnect()
        return False, f"📭 No posts found in the last 24h from {channel_username}."

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

    if collection.find_one({"username": username}):
        update.message.reply_text("⚠️ This channel is already saved in the database.")
        return

    try:
        chat = context.bot.get_chat(username)
        collection.insert_one({"username": username, "title": chat.title})
        update.message.reply_text(
            f"✅ <b>Channel saved successfully!</b>\n\n"
            f"📌 <b>Name:</b> {chat.title}\n"
            f"🔗 <b>Username:</b> {username}",
            parse_mode="HTML"
        )

        # Forward last 24h posts
        update.message.reply_text(f"⏳ Forwarding last 24h posts from {username}...")
        success, result_msg = asyncio.run(forward_last_24h(username))
        update.message.reply_text(result_msg, parse_mode="HTML")

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
            collection.update_one({"username": username}, {"$set": {"title": title}})
        except BadRequest:
            status = "❌"
            title = ch.get("title", "Unknown")

        msg_lines.append(f"{status} {username} — <b>{title}</b>")

    msg = "\n".join(msg_lines)
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
        collection.update_one({"username": username}, {"$set": {"title": chat.title}}, upsert=True)
    except BadRequest as e:
        update.message.reply_text(f"❌ Channel not found or inaccessible:\n<code>{str(e)}</code>", parse_mode="HTML")

# ======================
# Handle unknown commands
# ======================
def unknown_command(update, context):
    update.message.reply_text(
        "❌ Unknown command.\n\n"
        "👉 Available commands:\n"
        "/addchannel @ChannelUsername\n"
        "/listchannels\n"
        "/checkchannel @ChannelUsername"
    )

# ======================
# Main
# ======================
def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("addchannel", add_channel))
    dp.add_handler(CommandHandler("listchannels", list_channels))
    dp.add_handler(CommandHandler("checkchannel", check_channel))
    dp.add_handler(MessageHandler(Filters.command, unknown_command))  # catch unknown commands

    print("🚀 Bot is running...")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
