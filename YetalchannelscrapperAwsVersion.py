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
import boto3
import tempfile

# === 🔐 Load environment ===
load_dotenv()
API_ID = int(os.getenv("API_ID", "24916488"))
API_HASH = os.getenv("API_HASH", "3b7788498c56da1a02e904ff8e92d494")
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET", "telegram-scraper-bucket")

# === AWS S3 Setup ===
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# === MongoDB Setup ===
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["yetal"]
collection = db["yetalcollection"]

channels = [ch["username"] for ch in collection.find({})]
if not channels:
    print("⚠️ No channels found in DB. Add some with your bot first!")

USER_SESSION = "user_session"
BOT_SESSION = "bot_session"
TARGET_CHANNEL = "@Outis_ss1643"
FORWARDED_FILE = "forwarded_messages.json"

# === S3 Helper Functions ===
def upload_to_s3(file_path, s3_key):
    """Upload file to S3 bucket"""
    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"✅ Uploaded {file_path} to s3://{S3_BUCKET}/{s3_key}")
        return f"s3://{S3_BUCKET}/{s3_key}"
    except Exception as e:
        print(f"❌ Error uploading to S3: {e}")
        return None

def download_from_s3(s3_key, local_path):
    """Download file from S3"""
    try:
        s3_client.download_file(S3_BUCKET, s3_key, local_path)
        print(f"✅ Downloaded s3://{S3_BUCKET}/{s3_key} to {local_path}")
        return True
    except Exception as e:
        print(f"❌ Error downloading from S3: {e}")
        return False

def load_forwarded_ids_from_s3():
    """Load forwarded message IDs from S3"""
    try:
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
            temp_path = temp_file.name
        
        s3_client.download_file(S3_BUCKET, "state/forwarded_messages.json", temp_path)
        with open(temp_path, 'r') as f:
            data = json.load(f)
        os.unlink(temp_path)
        return {int(k): datetime.strptime(v, "%Y-%m-%d %H:%M:%S") for k, v in data.items()}
    except Exception as e:
        print(f"⚠️ No existing forwarded messages file or error loading: {e}")
        return {}

def save_forwarded_ids_to_s3(forwarded_ids):
    """Save forwarded message IDs to S3"""
    try:
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            json.dump({str(k): v.strftime("%Y-%m-%d %H:%M:%S") for k, v in forwarded_ids.items()}, temp_file)
            temp_path = temp_file.name
        
        s3_client.upload_file(temp_path, S3_BUCKET, "state/forwarded_messages.json")
        os.unlink(temp_path)
        print("✅ Saved forwarded IDs to S3")
    except Exception as e:
        print(f"❌ Error saving forwarded IDs: {e}")

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

# === 📦 Scraper Function (S3 Version) ===
async def scrape_and_save(client, timeframe="24h"):
    results = []  
    seen_posts = set()  

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24) if timeframe == "24h" else now - timedelta(days=7)

    # Use temporary directory for Lambda
    with tempfile.TemporaryDirectory() as temp_dir:
        download_dir = os.path.join(temp_dir, "downloaded_images")
        os.makedirs(download_dir, exist_ok=True)

        for channel in channels:
            print(f"📡 Scraping channel: {channel}")
            
            try:
                # ✅ FIX: Resolve channel entity first
                try:
                    channel_entity = await client.get_entity(channel)
                    print(f"✅ Channel resolved: {channel_entity.title}")
                except ValueError as e:
                    print(f"❌ Could not resolve channel {channel}: {e}")
                    continue
                except Exception as e:
                    print(f"❌ Error accessing channel {channel}: {e}")
                    continue
                
                safe_channel = channel.replace("@", "")
                channel_folder = os.path.join(download_dir, safe_channel)
                os.makedirs(channel_folder, exist_ok=True)

                # ✅ FIX: Use the resolved entity instead of the username string
                async for message in client.iter_messages(channel_entity, limit=None):
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
                            async for msg in client.iter_messages(channel_entity, limit=None, reverse=True):
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
                                        # Upload to S3 and store S3 URL
                                        s3_key = f"images/{safe_channel}/{os.path.basename(path)}"
                                        s3_url = upload_to_s3(path, s3_key)
                                        if s3_url:
                                            post_images.append(s3_url)
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
                                # Upload to S3 and store S3 URL
                                s3_key = f"images/{safe_channel}/{os.path.basename(path)}"
                                s3_url = upload_to_s3(path, s3_key)
                                if s3_url:
                                    post_images.append(s3_url)

                    except Exception as e:
                        print(f"❌ Error downloading/uploading image(s): {e}")

                    post_data = {
                        "title": info["title"],
                        "description": info["description"],
                        "price": info["price"],
                        "phone": info["phone"],
                        "images": post_images if post_images else None,
                        "location": info["location"],
                        "date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
                        "channel": info["channel_mention"] if info["channel_mention"] else channel,
                        "post_link": f"https://t.me/{channel.replace('@','')}/{message.id}"
                    }

                    results.append(post_data)

            except Exception as e:
                print(f"❌ Error processing channel {channel}: {e}")
                continue

        # Keep only posts newer than cutoff
        results = [
            post for post in results
            if datetime.strptime(post["date"], "%Y-%m-%d %H:%M:%S") >= cutoff.replace(tzinfo=None)
        ]

        # ✅ Add ID column
        for idx, post in enumerate(results, start=1):
            post["id"] = idx

        # === Save to Parquet and upload to S3 ===
        if results:
            df = pd.DataFrame(results)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_parquet = f"scraped_{timeframe}_{timestamp}.parquet"
            local_parquet_path = os.path.join(temp_dir, filename_parquet)
            
            df.to_parquet(local_parquet_path, engine="pyarrow", index=False)
            
            # Upload parquet to S3
            s3_parquet_key = f"parquet/{filename_parquet}"
            s3_url = upload_to_s3(local_parquet_path, s3_parquet_key)
            
            if s3_url:
                print(f"✅ Uploaded parquet file to: {s3_url}")
            else:
                print("❌ Failed to upload parquet file")

        print(f"\n✅ Done. Scraped {len(results)} posts ({timeframe}) from {len(channels)} channels.")
# === 📤 Forwarding Function with duplicate prevention & cleanup (S3 Version) ===
async def forward_messages(user, bot, days: int):
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)

    # Load previously forwarded messages with timestamps from S3
    forwarded_ids = load_forwarded_ids_from_s3()

    # Remove forwarded IDs older than specified days
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
           
    # Save updated forwarded IDs to S3
    save_forwarded_ids_to_s3(forwarded_ids)

    if total_forwarded > 0:
        print(f"\n✅ Done. Forwarded {total_forwarded} new posts ({days}d) to {TARGET_CHANNEL}.")
    else:
        print("\nℹ️ No new posts to forward. All messages already exist in the target channel.")
        
# === ⚡ Lambda Handler ===
async def async_main():
    """Async main function for Lambda"""
    user = TelegramClient(USER_SESSION, API_ID, API_HASH)
    await user.start()

    bot = TelegramClient(BOT_SESSION, API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)

    try:
        # Test channel access first
        print("🔍 Testing channel access...")
        for channel in channels:
            try:
                entity = await user.get_entity(channel)
                print(f"✅ {channel}: {entity.title}")
            except Exception as e:
                print(f"❌ {channel}: {e}")

        # 24h scrape → S3 parquet
        print("\nStarting 24-hour scrape to S3...")
        await scrape_and_save(user, timeframe="24h")

        # 7d scrape → S3 parquet
        print("\nStarting 7-day scrape to S3...")
        await scrape_and_save(user, timeframe="7d")

        # 7d forward → target channel
        print("\nStarting 7-day forwarding to channel...")
        await forward_messages(user, bot, days=7)

    finally:
        await user.disconnect()
        await bot.disconnect()

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    try:
        # Run the async main function
        asyncio.run(async_main())
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Telegram scraping completed successfully',
                'channels_processed': len(channels),
                'timestamp': datetime.now().isoformat()
            })
        }
    except Exception as e:
        print(f"❌ Lambda execution error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Telegram scraping failed',
                'timestamp': datetime.now().isoformat()
            })
        }

# === Local development (optional) ===
if __name__ == "__main__":
    # For local testing
    asyncio.run(async_main())