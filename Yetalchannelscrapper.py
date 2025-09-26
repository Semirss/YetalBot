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
mongo_client = MongoClient(
MONGO_URI, 
serverSelectionTimeoutMS=30000, 
connectTimeoutMS=50000,
socketTimeoutMS=50000
 )
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

def extract_info(text, message_id):
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
        "channel_mention": channel_mention,
        "product_ref": str(message_id) 
    }
# === 📦 Scraper Function (UPDATED VERSION) ===
async def scrape_and_save(client, timeframe="24h"):
    results = []  
    seen_posts = set()  

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24) if timeframe == "24h" else now - timedelta(days=7)
    
    # ✅ Resolve target channel (forwarded copies live here)
    try:
        target_entity = await client.get_entity(TARGET_CHANNEL)
        print(f"✅ Target channel resolved: {target_entity.title}")
    except Exception as e:
        print(f"❌ Could not resolve target channel {TARGET_CHANNEL}: {e}")
        return
    
    # ✅ FIRST: Collect all message texts from source channels to search for in target channel
    source_messages = []
    
    for channel in channels:
        print(f"📡 Scanning channel: {channel}")
        
        try:
            channel_entity = await client.get_entity(channel)
            print(f"✅ Channel resolved: {channel_entity.title}")
            
            async for message in client.iter_messages(channel_entity, limit=None):
                if not message.text:
                    continue

                if message.date < cutoff:
                    break

                source_messages.append({
                    'text': message.text,
                    'date': message.date,
                    'source_channel': channel,
                    'source_message_id': message.id
                })

        except Exception as e:
            print(f"❌ Error processing channel {channel}: {e}")
            continue

    # ✅ SECOND: Iterate through target channel and match messages
    print(f"🔍 Searching for matching messages in target channel...")
    
    async for message in client.iter_messages(target_entity, limit=None):
        if not message.text:
            continue

        if message.date < cutoff:
            break

        if message.id in seen_posts:
            continue
        seen_posts.add(message.id)

        # ✅ Find matching message from source channels
        matching_source = None
        for source_msg in source_messages:
            # Simple text matching - you might want to make this more robust
            if (source_msg['text'] in message.text or 
                message.text in source_msg['text'] or
                source_msg['text'][:100] in message.text):  # Match first 100 chars
                matching_source = source_msg
                break

        if not matching_source:
            continue

        info = extract_info(message.text, message.id)
        
        # ✅ Build permalink from TARGET channel (where the message actually exists)
        if getattr(target_entity, "username", None):
            post_link = f"https://t.me/{target_entity.username}/{message.id}"
        else:
            internal_id = str(target_entity.id)
            if internal_id.startswith("-100"):
                internal_id = internal_id[4:]
            post_link = f"https://t.me/c/{internal_id}/{message.id}"

        # Remove image downloading - keep empty images array
        post_images = []

        post_data = {
            "title": info["title"],
            "description": info["description"],
            "price": info["price"],
            "phone": info["phone"],
            "images": post_images,
            "location": info["location"],
            "date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
            "channel": info["channel_mention"] if info["channel_mention"] else matching_source['source_channel'],
            "post_link": post_link,   
            "product_ref": str(message.id)   # Use the target channel message ID
        }
        results.append(post_data)

    # Keep only posts newer than cutoff
    results = [
        post for post in results
        if datetime.strptime(post["date"], "%Y-%m-%d %H:%M:%S") >= cutoff.replace(tzinfo=None)
    ]

    # === Save to Parquet ===
    df = pd.DataFrame(results)
    filename_parquet = f"scraped_{timeframe}.parquet"
    df.to_parquet(filename_parquet, engine="pyarrow", index=False)

    print(f"\n✅ Done. Scraped {len(results)} posts ({timeframe}) from target channel.")
    print(f"📁 Data saved to {filename_parquet}")
# === 📤 Forwarding Function with duplicate prevention & cleanup ===
async def forward_messages(user, bot, days: int):
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)

    # Load previously forwarded messages with timestamps
    if os.path.exists(FORWARDED_FILE):
        with open(FORWARDED_FILE, "r") as f:
            forwarded_data = json.load(f)
            forwarded_ids = {
                int(msg_id): datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") 
                for msg_id, ts in forwarded_data.items()
            }
    else:
        forwarded_ids = {}

    # Remove forwarded IDs older than 7 days
    forwarded_ids = {msg_id: ts for msg_id, ts in forwarded_ids.items() if ts >= cutoff.replace(tzinfo=None)}

    messages_to_forward_by_channel = {channel: [] for channel in channels}

    # Collect messages
    for channel in channels:
        async for message in user.iter_messages(channel, limit=None):
            if message.date < cutoff:
                break
            if message.id not in forwarded_ids and (message.text or message.media):
                messages_to_forward_by_channel[channel].append(message)

    total_forwarded = 0
    for channel, messages_list in messages_to_forward_by_channel.items():
        if not messages_list:
            continue

        messages_list.reverse()
        for i in range(0, len(messages_list), 100):
            batch = messages_list[i:i+100]
            try:
                # Add timeout to avoid hanging forever
                await asyncio.wait_for(
                    bot.forward_messages(
                        entity=TARGET_CHANNEL,
                        messages=[msg.id for msg in batch],
                        from_peer=channel
                    ),
                    timeout=20
                )
                await asyncio.sleep(1)

                for msg in batch:
                    forwarded_ids[msg.id] = msg.date.replace(tzinfo=None)
                    total_forwarded += 1

            except ChatForwardsRestrictedError:
                print(f"🚫 Forwarding restricted for channel {channel}, skipping...")
                break
            except FloodWaitError as e:
                print(f"⏳ Flood wait error ({e.seconds}s). Waiting...")
                await asyncio.sleep(e.seconds)
                continue
            except asyncio.TimeoutError:
                print(f"⚠️ Forwarding timed out for {channel}, skipping batch...")
                continue
            except RPCError as e:
                print(f"⚠️ RPC Error for {channel}: {e}")
                continue
            except Exception as e:
                print(f"⚠️ Unexpected error forwarding from {channel}: {e}")
                continue
           
     # Save updated forwarded IDs
    with open(FORWARDED_FILE, "w") as f:
        json.dump({str(k): v.strftime("%Y-%m-%d %H:%M:%S") for k, v in forwarded_ids.items()}, f)

    if total_forwarded > 0:
        print(f"\n✅ Done. Forwarded {total_forwarded} new posts ({days}d) to {TARGET_CHANNEL}.")
    else:
        print("\nℹ️ No new posts to forward. All messages already exist in the target channel.")


# === ⚡ Main execution block ===
async def main():
    user = TelegramClient(USER_SESSION, API_ID, API_HASH)
    await user.start()

    bot = TelegramClient(BOT_SESSION, API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)

    # 24h scrape → parquet
    print("\nStarting 24-hour scrape to parquet...")
    await scrape_and_save(user, timeframe="24h")

    # 7d scrape → parquet
    print("\nStarting 7-day scrape to parquet...")
    await scrape_and_save(user, timeframe="7d")

    # 7d forward → target channel
    print("\nStarting 7-day forwarding to channel...")
    await forward_messages(user, bot, days=7)

    await user.disconnect()
    await bot.disconnect()



if __name__ == "__main__":
    asyncio.run(main())