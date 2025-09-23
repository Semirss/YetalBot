import os
import re
import json
import asyncio
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from telethon import TelegramClient
from dotenv import load_dotenv
import pandas as pd
from telethon.errors import ChatForwardsRestrictedError, FloodWaitError, RPCError
# === 🔐 Load environment ===
load_dotenv()
API_ID = int(os.getenv("API_ID", "24916488"))
API_HASH = os.getenv("API_HASH", "3b7788498c56da1a02e904ff8e92d494")
BOT_TOKEN = os.getenv("BOT_TOKEN")  # your bot token
MONGO_URI = os.getenv("MONGO_URI")

USER_SESSION = "user_session"
BOT_SESSION = "bot_session"
DOWNLOAD_DIR = "downloaded_images"
TARGET_CHANNEL = "@Outis_ss1643"
FORWARDED_FILE = "forwarded_messages.json"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# === ⚡ MongoDB Setup ===
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["yetal"]
collection = db["yetalcollection"]

channels = [ch["username"] for ch in collection.find({})]
if not channels:
    print("⚠️ No channels found in DB. Add some with your bot first!")
    exit()

# === 🧹 Helpers ===
def clean_text(text):
    return ' '.join(text.replace('\xa0', ' ').split())

def extract_info(text):
    text = clean_text(text)
    
    title_match = re.split(r'\n|💸|☘️☘️PRICE|Price\s*:|💵', text)[0].strip()
    title = title_match[:100] if title_match else "No Title"
    
    phone_matches = re.findall(r'(\+251\d{8,9}|09\d{8})', text)
    phone = phone_matches[0] if phone_matches else ""
    
    price_match = re.search(
        r'(Price|💸|☘️☘️PRICE)[:\s]*([\d,]+)|([\d,]+)\s*(ETB|Birr|birr|💵)', 
        text, 
        re.IGNORECASE
    )
    price = ""
    if price_match:
        price = price_match.group(2) or price_match.group(3) or ""
        price = price.replace(',', '').strip()
    
    location_match = re.search(
        r'(📍|Address|Location|🌺🌺)[:\s]*(.+?)(?=\n|☘️|📞|@|$)', 
        text, 
        re.IGNORECASE
    )
    location = location_match.group(2).strip() if location_match else ""
    
    channel_mention = re.search(r'(@\w+)', text)
    channel_mention = channel_mention.group(1) if channel_mention else ""
    
    return {
        "title": title,
        "description": text,
        "price": price,
        "phone": phone,
        "location": location,
        "channel_mention": channel_mention
    }

# === 📦 Scraper Function (fixed for multiple images) ===
async def scrape_and_save(client, timeframe="24h"):
    results = []  
    seen_posts = set()  

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24) if timeframe == "24h" else now - timedelta(days=7)

    for channel in channels:
        print(f"📡 Scraping channel: {channel}")
        safe_channel = channel.replace("@", "")
        channel_folder = os.path.join(DOWNLOAD_DIR, safe_channel)
        os.makedirs(channel_folder, exist_ok=True)

        async for message in client.iter_messages(channel, limit=None):
            if not message.text:
                continue

            if message.date < cutoff:
                break

            if (channel, message.id) in seen_posts:
                continue
            seen_posts.add((channel, message.id))

            info = extract_info(message.text)
            post_images = []

            # === Handle single media and albums properly ===
            try:
                # If message is part of an album
                if getattr(message, "grouped_id", None):
                    async for msg in client.iter_messages(channel, limit=None, reverse=True):
                        if getattr(msg, "grouped_id", None) == message.grouped_id:
                            clean_title = re.sub(r'[^\w\-_. ]', '_', info['title'])[:30]
                            ext = "jpg"
                            if msg.photo:
                                path = await client.download_media(
                                    msg.photo,
                                    file=os.path.join(channel_folder, f"{clean_title}_{msg.id}.jpg")
                                )
                            elif msg.document:
                                ext = msg.file.name.split('.')[-1] if msg.file else "dat"
                                path = await client.download_media(
                                    msg.document,
                                    file=os.path.join(channel_folder, f"{clean_title}_{msg.id}.{ext}")
                                )
                            else:
                                continue
                            if path:
                                post_images.append(path.replace('\\', '/'))
                else:
                    clean_title = re.sub(r'[^\w\-_. ]', '_', info['title'])[:30]
                    ext = "jpg"
                    if message.photo:
                        path = await client.download_media(
                            message.photo,
                            file=os.path.join(channel_folder, f"{clean_title}_{message.id}.jpg")
                        )
                    elif message.document:
                        ext = message.file.name.split('.')[-1] if message.file else "dat"
                        path = await client.download_media(
                            message.document,
                            file=os.path.join(channel_folder, f"{clean_title}_{message.id}.{ext}")
                        )
                    else:
                        path = None
                    if path:
                        post_images.append(path.replace('\\', '/'))

            except Exception as e:
                print(f"❌ Error downloading image(s): {e}")

            post_data = {
                "title": info["title"],
                "description": info["description"],
                "price": info["price"],
                "phone": info["phone"],
                "images": post_images if post_images else None,
                "location": info["location"],
                "date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
                "channel": info["channel_mention"] if info["channel_mention"] else channel
            }

            results.append(post_data)

        # Cleanup old images only for 7-day scrape
        if timeframe == "7d":
            for file in os.listdir(channel_folder):
                file_path = os.path.join(channel_folder, file)
                if os.path.isfile(file_path):
                    file_mtime = datetime.utcfromtimestamp(os.path.getmtime(file_path))
                    if file_mtime < cutoff.replace(tzinfo=None):
                        os.remove(file_path)
                        print(f"🗑️ Deleted old image: {file_path}")

    # Keep only posts newer than cutoff
    results = [
        post for post in results
        if datetime.strptime(post["date"], "%Y-%m-%d %H:%M:%S") >= cutoff.replace(tzinfo=None)
    ]

    # ✅ Add ID column
    for idx, post in enumerate(results, start=1):
        post["id"] = idx

 
    # === Save to Parquet ===
    df = pd.DataFrame(results)
    filename_parquet = f"scraped_{timeframe}.parquet"
    df.to_parquet(filename_parquet, engine="pyarrow", index=False)

    print(f"\n✅ Done. Scraped {len(results)} posts ({timeframe}) from {len(channels)} channels.")
    print(f"📁 {filename_parquet}, images → /{DOWNLOAD_DIR}/")

# === Fixed forwarding: preserves albums and correctly uses the `days` window ===
async def forward_messages(user, bot, days: int):
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)

    # --- Load previously forwarded IDs (UTC-aware datetimes) ---
    forwarded_ids = {}
    if os.path.exists(FORWARDED_FILE):
        with open(FORWARDED_FILE, "r", encoding="utf-8") as f:
            forwarded_data = json.load(f)
            for key, ts in forwarded_data.items():
                try:
                    forwarded_ids[key] = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except Exception:
                    # ignore unparsable entries
                    continue

    # Keep only recent forwarded entries (within window)
    forwarded_ids = {k: v for k, v in forwarded_ids.items() if v >= cutoff}

    total_forwarded = 0

    for channel in channels:
        # Collect all messages in the window for this channel (newest -> oldest)
        messages = []
        async for message in user.iter_messages(channel, limit=None):
            # ensure message.date is timezone-aware
            msg_date = message.date
            if getattr(msg_date, "tzinfo", None) is None:
                msg_date = msg_date.replace(tzinfo=timezone.utc)

            if msg_date < cutoff:
                break   # we've reached messages older than cutoff; stop iterating this channel

            messages.append(message)

        if not messages:
            continue

        # Process messages oldest -> newest so forwards appear natural
        processed_groups = set()
        for message in reversed(messages):
            # skip empty messages
            if not (getattr(message, "text", None) or getattr(message, "media", None)):
                continue

            grouped_id = getattr(message, "grouped_id", None)
            if grouped_id:
                if grouped_id in processed_groups:
                    continue

                # Collect album parts found inside the window
                album_messages = [m for m in messages if getattr(m, "grouped_id", None) == grouped_id]

                max_scan = 500
                scanned = 0
                try:
                    # Start from the oldest album message we currently have
                    oldest_in_buffer = album_messages[0] if album_messages else message
                    async for old_msg in user.iter_messages(channel, offset_id=oldest_in_buffer.id, limit=None):
                        scanned += 1
                        if scanned > max_scan:
                            break
                        if getattr(old_msg, "grouped_id", None) == grouped_id:
                            album_messages.insert(0, old_msg)  # prepend older part
                        else:
                            # stop scanning older messages when grouped_id no longer matches
                            # (most albums are contiguous)
                            break
                except Exception:
                    # If fetching older parts fails for any reason, proceed with what we have
                    pass

                # sort by id ascending to maintain original order
                album_messages.sort(key=lambda x: x.id)

                # If any message in this album was already forwarded, skip whole album
                if any(f"{channel}:{m.id}" in forwarded_ids for m in album_messages):
                    processed_groups.add(grouped_id)
                    continue

                # Forward album as a single post by sending the list of Message objects
                try:
                    await bot.forward_messages(
                        entity=TARGET_CHANNEL,
                        messages=album_messages,
                        from_peer=channel
                    )

                    for m in album_messages:
                        key = f"{channel}:{m.id}"
                        m_date = m.date
                        if getattr(m_date, "tzinfo", None) is None:
                            m_date = m_date.replace(tzinfo=timezone.utc)
                        forwarded_ids[key] = m_date

                    processed_groups.add(grouped_id)
                    total_forwarded += len(album_messages)
                    print(f"✅ Forwarded album ({len(album_messages)}) from {channel}")
                    await asyncio.sleep(0.5)

                except ChatForwardsRestrictedError:
                    print(f"🚫 Forwarding restricted for {channel}, skipping channel...")
                    break
                except FloodWaitError as e:
                    print(f"⚠️ Flood wait ({e.seconds}s) for {channel}, skipping this message...")
                    continue
                except RPCError as e:
                    print(f"⚠️ RPC error for {channel}: {e}, skipping...")
                    continue
                except Exception as e:
                    print(f"⚠️ Unexpected error for {channel}: {e}, skipping...")
                    continue

            else:
                # Single non-album message
                unique_key = f"{channel}:{message.id}"
                if unique_key in forwarded_ids:
                    continue

                try:
                    await bot.forward_messages(
                        entity=TARGET_CHANNEL,
                        messages=message,
                        from_peer=channel
                    )
                    m_date = message.date
                    if getattr(m_date, "tzinfo", None) is None:
                        m_date = m_date.replace(tzinfo=timezone.utc)
                    forwarded_ids[unique_key] = m_date
                    total_forwarded += 1
                    print(f"✅ Forwarded single message from {channel}")
                    await asyncio.sleep(0.5)

                except ChatForwardsRestrictedError:
                    print(f"🚫 Forwarding restricted for {channel}, skipping channel...")
                    break
                except FloodWaitError as e:
                    print(f"⚠️ Flood wait ({e.seconds}s) for {channel}, skipping this message...")
                    continue
                except RPCError as e:
                    print(f"⚠️ RPC error for {channel}: {e}, skipping...")
                    continue
                except Exception as e:
                    print(f"⚠️ Unexpected error for {channel}: {e}, skipping...")
                    continue

    # Persist forwarded IDs (store as UTC strings)
    with open(FORWARDED_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {k: v.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") for k, v in forwarded_ids.items()},
            f,
            indent=2,
            ensure_ascii=False
        )

    print(f"\n✅ Done. Forwarded {total_forwarded} new posts ({days}d) → {TARGET_CHANNEL}")

# === ⚡ Main execution block ===
async def main():
    user = TelegramClient(USER_SESSION, API_ID, API_HASH)
    await user.start()

    bot = TelegramClient(BOT_SESSION, API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)

    # # 24h scrape → JSON
    # print("Starting 24-hour scrape to JSON...")
    # await scrape_and_save(user, timeframe="24h")

    # # 7d scrape → JSON
    # print("\nStarting 7-day scrape to JSON...")
    # await scrape_and_save(user, timeframe="7d")

    # 7d forward → target channel
    print("\nStarting 7-day forwarding to channel...")
    await forward_messages(user, bot, days=7)

    await user.disconnect()
    await bot.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
