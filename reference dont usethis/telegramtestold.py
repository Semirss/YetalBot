import json
import re
import os
from datetime import datetime, timedelta
from pymongo import MongoClient
from telethon.sync import TelegramClient
from dotenv import load_dotenv

# === 🔐 Load environment variables ===
load_dotenv()
api_id = 24916488    # your API ID
api_hash = '3b7788498c56da1a02e904ff8e92d494'  # your API Hash
MONGO_URI = os.getenv("MONGO_URI")  

# === 📂 Create base image folder ===
os.makedirs("downloaded_images", exist_ok=True)

# === ⚡ MongoDB Setup ===
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["yetal"]
collection = db["yetalcollection"]

# === 🗂️ Load channels from DB ===
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

# === 🛠️ Scraper Function ===
def scrape(timeframe="24h"):
    """
    Scrapes posts from Telegram channels.
    timeframe: '24h' or '7d'
    """
    results = []  # Flat list of posts
    seen_posts = set()  # Avoid duplicates: (channel, message_id)

    now = datetime.utcnow()
    cutoff = None
    if timeframe == "24h":
        cutoff = now - timedelta(hours=24)
    elif timeframe == "7d":
        cutoff = now - timedelta(days=7)

    with TelegramClient('anon', api_id, api_hash) as client:
        for channel in channels:
            print(f"📡 Scraping channel: {channel}")
            safe_channel = channel.replace("@", "")
            channel_folder = os.path.join("downloaded_images", safe_channel)
            os.makedirs(channel_folder, exist_ok=True)

            # Iterate messages and stop when reaching cutoff
            for message in client.iter_messages(channel, limit=None):
                if not message.text:
                    continue

                # Stop if older than cutoff
                if cutoff and message.date.replace(tzinfo=None) < cutoff:
                    break

                # Skip duplicates
                if (channel, message.id) in seen_posts:
                    continue
                seen_posts.add((channel, message.id))

                info = extract_info(message.text)
                
                post_images = []
                if message.media:
                    try:
                        media_list = message.media if isinstance(message.media, list) else [message.media]
                        for idx, media_item in enumerate(media_list):
                            clean_title = re.sub(r'[^\w\-_. ]', '_', info['title'])[:30]
                            safe_filename = f"{clean_title}_{message.id}_{idx}.jpg"
                            path = client.download_media(
                                media_item,
                                file=os.path.join(channel_folder, safe_filename)
                            )
                            post_images.append(path.replace('\\', '/'))
                    except Exception as e:
                        print(f"❌ Error downloading image: {e}")

                post_data = {
                    "title": info["title"],
                    "description": info["description"],
                    "price": info["price"],
                    "phone": info["phone"],
                    "location": info["location"],
                    "images": post_images if post_images else None,
                    "date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
                    "channel": info["channel_mention"] if info["channel_mention"] else channel
                }

                results.append(post_data)

            # Cleanup old images for 7-day scraping
            if timeframe == "7d":
                for file in os.listdir(channel_folder):
                    file_path = os.path.join(channel_folder, file)
                    if os.path.isfile(file_path):
                        file_mtime = datetime.utcfromtimestamp(os.path.getmtime(file_path))
                        if file_mtime < cutoff:
                            os.remove(file_path)

    # Save JSON
    filename = f"scraped_{timeframe}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Done. Scraped {len(results)} posts ({timeframe}) from {len(channels)} channels.")
    print(f"📁 Data saved to {filename} and images downloaded to /downloaded_images/")

# === 🚀 Run Scraper ===
if __name__ == "__main__":
    scrape("24h")  # JSON for last 24 hours
    scrape("7d")   # JSON for last 7 days
