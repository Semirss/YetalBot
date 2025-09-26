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
from io import BytesIO

# === 🔐 Load environment ===
load_dotenv()
API_ID = int(os.getenv("API_ID", "24916488"))
API_HASH = os.getenv("API_HASH", "3b7788498c56da1a02e904ff8e92d494")
BOT_TOKEN = os.getenv("BOT_TOKEN")  # your bot token
MONGO_URI = os.getenv("MONGO_URI")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "telegram-scraper-bucket")

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

# === ☁️ S3 Storage Setup ===
s3_client = None
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        print("✅ S3 client initialized successfully")
    except Exception as e:
        print(f"❌ S3 client initialization failed: {e}")
else:
    print("⚠️ AWS credentials not found, S3 functionality disabled")

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

# === ☁️ S3 Storage Functions ===
def upload_to_s3(file_path, s3_key):
    """Upload a file to S3 bucket"""
    if not s3_client:
        print("⚠️ S3 client not available, skipping upload")
        return None
    try:
        s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_key)
        print(f"✅ Uploaded {file_path} to s3://{S3_BUCKET_NAME}/{s3_key}")
        return f"s3://{S3_BUCKET_NAME}/{s3_key}"
    except Exception as e:
        print(f"❌ Error uploading to S3: {e}")
        return None

def upload_dataframe_to_s3(df, s3_key):
    """Upload DataFrame directly to S3 as parquet"""
    if not s3_client:
        print("⚠️ S3 client not available, skipping upload")
        return None
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=buffer
        )
        print(f"✅ Uploaded DataFrame to s3://{S3_BUCKET_NAME}/{s3_key}")
        return f"s3://{S3_BUCKET_NAME}/{s3_key}"
    except Exception as e:
        print(f"❌ Error uploading DataFrame to S3: {e}")
        return None

def download_from_s3(s3_key, local_path):
    """Download a file from S3 bucket"""
    if not s3_client:
        print("⚠️ S3 client not available, skipping download")
        return None
    try:
        s3_client.download_file(S3_BUCKET_NAME, s3_key, local_path)
        print(f"✅ Downloaded s3://{S3_BUCKET_NAME}/{s3_key} to {local_path}")
        return local_path
    except Exception as e:
        print(f"❌ Error downloading from S3: {e}")
        return None

def list_s3_files(prefix=""):
    """List files in S3 bucket with optional prefix"""
    if not s3_client:
        print("⚠️ S3 client not available, skipping list operation")
        return []
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        return []
    except Exception as e:
        print(f"❌ Error listing S3 files: {e}")
        return []

def upload_images_to_s3(image_paths, s3_prefix):
    """Upload multiple images to S3 with a common prefix"""
    s3_urls = []
    if not s3_client:
        print("⚠️ S3 client not available, skipping image upload")
        return s3_urls
        
    for image_path in image_paths:
        if image_path and os.path.exists(image_path):
            s3_key = f"{s3_prefix}/{os.path.basename(image_path)}"
            s3_url = upload_to_s3(image_path, s3_key)
            if s3_url:
                s3_urls.append(s3_url)
    return s3_urls

# === 📦 Scraper Function (FIXED VERSION) ===
async def scrape_and_save(client, timeframe="24h"):
    results = []  
    seen_posts = set()  

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24) if timeframe == "24h" else now - timedelta(days=7)

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
            channel_folder = os.path.join(DOWNLOAD_DIR, safe_channel)
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

                # Upload images to S3 if any (NEW FUNCTIONALITY)
                s3_image_urls = []
                if post_images and s3_client:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    s3_prefix = f"images/{safe_channel}/{timestamp}_{message.id}"
                    s3_image_urls = upload_images_to_s3(post_images, s3_prefix)

                post_data = {
                    "title": info["title"],
                    "description": info["description"],
                    "price": info["price"],
                    "phone": info["phone"],
                    "images": post_images if post_images else None,
                    "s3_image_urls": s3_image_urls if s3_image_urls else None,  # NEW FIELD
                    "location": info["location"],
                    "date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
                    "channel": info["channel_mention"] if info["channel_mention"] else channel,
                    "post_link": f"https://t.me/{channel.replace('@','')}/{message.id}"
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

    # === Save to Parquet ===
    df = pd.DataFrame(results)
    filename_parquet = f"scraped_{timeframe}.parquet"
    df.to_parquet(filename_parquet, engine="pyarrow", index=False)
    
    # === Upload to S3 (NEW FUNCTIONALITY) ===
    if s3_client:
        s3_key = f"scraped_data/{datetime.now().strftime('%Y/%m/%d')}/scraped_{timeframe}_{datetime.now().strftime('%H%M%S')}.parquet"
        upload_dataframe_to_s3(df, s3_key)
    else:
        print("⚠️ S3 upload skipped - no S3 client available")

    print(f"\n✅ Done. Scraped {len(results)} posts ({timeframe}) from {len(channels)} channels.")
    print(f"📁 Data saved to {filename_parquet}, images → /{DOWNLOAD_DIR}/")
    if s3_client:
        print(f"☁️ Data uploaded to S3: {s3_key}")


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

# === ☁️ Lambda Handler for S3 Storage ===
def lambda_handler(event, context):
    """
    AWS Lambda handler for S3 storage operations
    """
    try:
        # Initialize S3 client for Lambda
        if not s3_client and AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            s3_client_lambda = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
        else:
            s3_client_lambda = s3_client
            
        if not s3_client_lambda:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'S3 client not available'
                })
            }

        # Determine operation from event
        operation = event.get('operation', 'upload_scraped_data')
        
        if operation == 'upload_scraped_data':
            # This would typically be triggered after scraping
            timeframe = event.get('timeframe', '24h')
            local_file = f"scraped_{timeframe}.parquet"
            
            if os.path.exists(local_file):
                s3_key = f"scraped_data/{datetime.now().strftime('%Y/%m/%d')}/scraped_{timeframe}_{datetime.now().strftime('%H%M%S')}.parquet"
                
                try:
                    s3_client_lambda.upload_file(local_file, S3_BUCKET_NAME, s3_key)
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'message': f'Successfully uploaded {timeframe} data to S3',
                            's3_location': f"s3://{S3_BUCKET_NAME}/{s3_key}",
                            'timeframe': timeframe
                        })
                    }
                except Exception as e:
                    return {
                        'statusCode': 500,
                        'body': json.dumps({
                            'message': f'Error uploading to S3: {str(e)}'
                        })
                    }
            else:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'message': f'Local file {local_file} not found'
                    })
                }
                
        elif operation == 'list_files':
            prefix = event.get('prefix', '')
            try:
                response = s3_client_lambda.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
                files = [obj['Key'] for obj in response.get('Contents', [])]
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': f'Found {len(files)} files in S3',
                        'files': files,
                        'prefix': prefix
                    })
                }
            except Exception as e:
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'message': f'Error listing S3 files: {str(e)}'
                    })
                }
            
        elif operation == 'download_file':
            s3_key = event.get('s3_key')
            local_path = event.get('local_path', '/tmp/downloaded_file.parquet')
            
            if not s3_key:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'message': 's3_key parameter is required'
                    })
                }
                
            try:
                s3_client_lambda.download_file(S3_BUCKET_NAME, s3_key, local_path)
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': f'Successfully downloaded file from S3',
                        'local_path': local_path,
                        's3_key': s3_key
                    })
                }
            except Exception as e:
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'message': f'Error downloading from S3: {str(e)}'
                    })
                }
                
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': f'Unknown operation: {operation}'
                })
            }
            
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error in Lambda handler: {str(e)}'
            })
        }

# === ⚡ Main execution block ===
async def main():
    user = TelegramClient(USER_SESSION, API_ID, API_HASH)
    await user.start()

    bot = TelegramClient(BOT_SESSION, API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)

    # Test channel access first
    print("🔍 Testing channel access...")
    for channel in channels:
        try:
            entity = await user.get_entity(channel)
            print(f"✅ {channel}: {entity.title}")
        except Exception as e:
            print(f"❌ {channel}: {e}")

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