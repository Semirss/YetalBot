import os
import asyncio
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pymongo import MongoClient
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from telegram.error import BadRequest
from telethon import TelegramClient
from telethon.errors import ChannelInvalidError, UsernameInvalidError, UsernameNotOccupiedError
import threading
import glob

# === 🔐 Load environment variables ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
API_ID = 24916488
API_HASH = "3b7788498c56da1a02e904ff8e92d494"
FORWARD_CHANNEL = os.getenv("FORWARD_CHANNEL")  # target channel username
ADMIN_CODE = os.getenv("ADMIN_CODE")  # secret code for access

# === ⚡ MongoDB Setup ===
client = MongoClient(MONGO_URI)
db = client["yetal"]
channels_collection = db["yetalcollection"]
auth_collection = db["authorized_users"]  # store authorized user IDs

def cleanup_telethon_sessions(channel_username=None):
    """Clean up Telethon session files for a specific channel or all temporary sessions"""
    try:
        if channel_username:
            # Clean up specific session files
            session_pattern = f"session_{channel_username}.*"
            files = glob.glob(session_pattern)
            for file in files:
                os.remove(file)
                print(f"🧹 Deleted session file: {file}")
        else:
            # Clean up all temporary session files (optional)
            # This can be used for general cleanup if needed
            session_files = glob.glob("session_*.*")
            for file in session_files:
                # Don't delete the main user session
                if not file.startswith("session_"):
                    continue
                os.remove(file)
                print(f"🧹 Deleted session file: {file}")
    except Exception as e:
        print(f"❌ Error cleaning up session files: {e}")

# ======================
# Forward last 24h posts from a given channel
# ======================
async def forward_last_24h_async(channel_username: str):
    """Async function to forward messages using a dedicated Telethon client"""
    telethon_client = None
    session_name = f"session_{channel_username}"
    
    try:
        # Create a new Telethon client for this operation
        telethon_client = TelegramClient(session_name, API_ID, API_HASH)
        await telethon_client.start()
        
        print(f"🔍 Checking if channel {channel_username} exists...")
        
        # First, verify the channel exists and we can access it
        try:
            entity = await telethon_client.get_entity(channel_username)
            print(f"✅ Channel found: {entity.title}")
        except (ChannelInvalidError, UsernameInvalidError, UsernameNotOccupiedError) as e:
            print(f"❌ Channel error: {e}")
            return False, f"❌ Channel {channel_username} is invalid or doesn't exist."
        except Exception as e:
            print(f"❌ Unexpected error getting entity: {e}")
            return False, f"❌ Error accessing channel: {str(e)}"

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=24)
        print(f"⏰ Cutoff time: {cutoff}")

        messages_to_forward = []
        message_count = 0
        
        print(f"📨 Fetching messages from {channel_username}...")
        async for message in telethon_client.iter_messages(channel_username, limit=100):
            message_count += 1
            if message_count % 10 == 0:
                print(f"📊 Processed {message_count} messages...")
                
            if message.date < cutoff:
                print(f"⏹️ Reached cutoff time at message {message_count}")
                break
            if message.text or message.media:
                messages_to_forward.append(message)
                print(f"✅ Added message from {message.date}")

        print(f"📋 Found {len(messages_to_forward)} messages to forward")

        if messages_to_forward:
            messages_to_forward.reverse()
            print(f"➡️ Forwarding {len(messages_to_forward)} messages from {channel_username}...")
            
            success_count = 0
            for i, message in enumerate(messages_to_forward):
                try:
                    await telethon_client.forward_messages(
                        entity=FORWARD_CHANNEL,
                        messages=message.id,
                        from_peer=channel_username,
                    )
                    success_count += 1
                    print(f"✅ Forwarded message {i+1}/{len(messages_to_forward)}")
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    print(f"❌ Error forwarding message {i+1}: {e}")

            return True, f"✅ Successfully forwarded {success_count}/{len(messages_to_forward)} posts from {channel_username}."
        else:
            return False, f"📭 No posts found in the last 24h from {channel_username}."

    except Exception as e:
        print(f"❌ Critical error in forward_last_24h: {e}")
        return False, f"❌ Critical error: {str(e)}"
    finally:
        if telethon_client:
            await telethon_client.disconnect()
            print("✅ Telethon client disconnected")
            
        # Clean up session files after operation
        cleanup_telethon_sessions(channel_username)

def forward_last_24h_sync(channel_username: str):
    """Synchronous wrapper for the async forwarding function"""
    try:
        # Create a new event loop for this operation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        result = loop.run_until_complete(forward_last_24h_async(channel_username))
        loop.close()
        return result
    except Exception as e:
        print(f"❌ Error in forward_last_24h_sync: {e}")
        return False, f"❌ Error: {str(e)}"

# ======================
# /start command
# ======================
def start(update, context):
    user_id = update.effective_user.id
    if auth_collection.find_one({"user_id": user_id}):
        update.message.reply_text(
            "✅ You are already authorized!\nYou can now use the bot commands."
        )
    else:
        update.message.reply_text(
            "⚡ Welcome! Please enter your access code using /code YOUR_CODE"
        )

# ======================
# /code command
# ======================
def code(update, context):
    user_id = update.effective_user.id
    if auth_collection.find_one({"user_id": user_id}):
        update.message.reply_text("✅ You are already authorized!")
        return

    if len(context.args) == 0:
        update.message.reply_text("⚠️ Usage: /code YOUR_ACCESS_CODE")
        return

    entered_code = context.args[0].strip()
    if entered_code == ADMIN_CODE:
        auth_collection.insert_one({"user_id": user_id})
        update.message.reply_text("✅ Code accepted! You can now use the bot commands.")
    else:
        update.message.reply_text("❌ Invalid code. Access denied.")

# ======================
# Wrapper for command authorization
# ======================
def authorized(func):
    def wrapper(update, context, *args, **kwargs):
        user_id = update.effective_user.id
        if not auth_collection.find_one({"user_id": user_id}):
            update.message.reply_text(
                "❌ You must enter a valid code first. Use /start to begin."
            )
            return
        return func(update, context, *args, **kwargs)

    return wrapper

# ======================
# Bot commands
# ======================
@authorized
def add_channel(update, context):
    if len(context.args) == 0:
        update.message.reply_text("⚡ Usage: /addchannel @ChannelUsername")
        return

    username = context.args[0].strip()
    if not username.startswith("@"):
        update.message.reply_text(
            "❌ Please provide a valid channel username starting with @"
        )
        return

    if channels_collection.find_one({"username": username}):
        update.message.reply_text("⚠️ This channel is already saved in the database.")
        return

    try:
        chat = context.bot.get_chat(username)
        channels_collection.insert_one({"username": username, "title": chat.title})
        update.message.reply_text(
            f"✅ <b>Channel saved successfully!</b>\n\n"
            f"📌 <b>Name:</b> {chat.title}\n"
            f"🔗 <b>Username:</b> {username}",
            parse_mode="HTML",
        )

        # Forward last 24h posts
        update.message.reply_text(f"⏳ Forwarding last 24h posts from {username}...")
        print(f"🚀 Starting forwarding process for {username}")

        # Run forwarding in a separate thread to avoid blocking
        def run_forwarding():
            try:
                success, result_msg = forward_last_24h_sync(username)
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=result_msg,
                    parse_mode="HTML"
                )
                print(f"📤 Sent result: {result_msg}")
            except Exception as e:
                error_msg = f"❌ Error during forwarding: {str(e)}"
                print(error_msg)
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=error_msg,
                    parse_mode="HTML"
                )

        # Start forwarding in a background thread
        threading.Thread(target=run_forwarding, daemon=True).start()

    except BadRequest as e:
        update.message.reply_text(
            f"❌ Could not add channel:\n<code>{str(e)}</code>", parse_mode="HTML"
        )
    except Exception as e:
        update.message.reply_text(
            f"❌ Unexpected error:\n<code>{str(e)}</code>", parse_mode="HTML"
        )

@authorized
def list_channels(update, context):
    channels = list(channels_collection.find({}))
    if not channels:
        update.message.reply_text("📭 No channels saved yet.")
        return

    msg_lines = ["📃 <b>Saved Channels:</b>\n"]
    for ch in channels:
        username = ch.get("username")
        title = ch.get("title", "Unknown")
        msg_lines.append(f"{username} — <b>{title}</b>")

    msg = "\n".join(msg_lines)
    for chunk in [msg[i : i + 4000] for i in range(0, len(msg), 4000)]:
        update.message.reply_text(chunk, parse_mode="HTML")

@authorized
def check_channel(update, context):
    if len(context.args) == 0:
        update.message.reply_text("⚡ Usage: /checkchannel @ChannelUsername")
        return

    username = context.args[0].strip()
    if not username.startswith("@"):
        update.message.reply_text(
            "❌ Please provide a valid channel username starting with @"
        )
        return

    doc = channels_collection.find_one({"username": username})
    if doc:
        update.message.reply_text(
            f"🔍 <b>Channel found in database!</b>\n\n"
            f"📌 <b>Name:</b> {doc.get('title', 'Unknown')}\n"
            f"🔗 <b>Username:</b> {username}",
            parse_mode="HTML",
        )
    else:
        update.message.reply_text(
            f"❌ Channel {username} is not in the database.", parse_mode="HTML"
        )

@authorized
def delete_channel(update, context):
    if len(context.args) == 0:
        update.message.reply_text("⚡ Usage: /deletechannel @ChannelUsername")
        return

    username = context.args[0].strip()
    if not username.startswith("@"):
        update.message.reply_text(
            "❌ Please provide a valid channel username starting with @"
        )
        return

    result = channels_collection.delete_one({"username": username})
    if result.deleted_count > 0:
        update.message.reply_text(
            f"✅ Channel {username} has been deleted from the database."
        )
    else:
        update.message.reply_text(
            f"⚠️ Channel {username} was not found in the database."
        )

@authorized
def unknown_command(update, context):
    update.message.reply_text(
        "❌ Unknown command.\n\n"
        "👉 Available commands:\n"
        "/addchannel @ChannelUsername\n"
        "/listchannels\n"
        "/checkchannel @ChannelUsername\n"
        "/deletechannel @ChannelUsername"
    )

# ======================
# Test command to check telethon connection
# ======================
@authorized
def test_connection(update, context):
    """Test if Telethon client is working"""
    def run_test():
        try:
            # Test with a fresh client instance
            async def test_async():
                try:
                    telethon_client = TelegramClient("test_session", API_ID, API_HASH)
                    await telethon_client.start()
                    
                    me = await telethon_client.get_me()
                    result = f"✅ Telethon connected as: {me.first_name} (@{me.username})"
                    
                    try:
                        target = await telethon_client.get_entity(FORWARD_CHANNEL)
                        result += f"\n✅ Target channel accessible: {target.title}"
                    except Exception as e:
                        result += f"\n❌ Cannot access target channel {FORWARD_CHANNEL}: {e}"
                    
                    await telethon_client.disconnect()
                    return result
                except Exception as e:
                    return f"❌ Telethon connection error: {e}"
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_async())
            loop.close()
            
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=result
            )
            
        except Exception as e:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Test failed: {e}"
            )
    
    # Run test in background thread
    threading.Thread(target=run_test, daemon=True).start()

# ======================
# Cleanup command to remove all temporary session files
# ======================
@authorized
def cleanup_sessions(update, context):
    """Clean up all temporary Telethon session files"""
    try:
        cleanup_telethon_sessions()
        update.message.reply_text("✅ All temporary session files have been cleaned up.")
    except Exception as e:
        update.message.reply_text(f"❌ Error cleaning up sessions: {e}")

# ======================
# Main
# ======================
def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("code", code))
    dp.add_handler(CommandHandler("addchannel", add_channel))
    dp.add_handler(CommandHandler("listchannels", list_channels))
    dp.add_handler(CommandHandler("checkchannel", check_channel))
    dp.add_handler(CommandHandler("deletechannel", delete_channel))
    dp.add_handler(CommandHandler("test", test_connection))
    dp.add_handler(CommandHandler("cleanup", cleanup_sessions))
    dp.add_handler(MessageHandler(Filters.command, unknown_command))

    print("🤖 Bot is running...")
    
    try:
        updater.start_polling()
        updater.idle()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down bot...")
    except Exception as e:
        print(f"❌ Bot error: {e}")
    finally:
        print("👋 Bot stopped")

if __name__ == "__main__":
    main()