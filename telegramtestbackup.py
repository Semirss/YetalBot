import json
import re
import os
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from datetime import datetime

# === 🔐 Telegram API credentials ===
api_id = 24916488    # your API ID
api_hash = '3b7788498c56da1a02e904ff8e92d494'  # your API Hash

# === 📡 List of Telegram channels to scrape ===
channels = [
    '@CosmeticsworldEthiopia',
    '@heyonlinemarket',
    '@samia_s',
    '@Afrocosmeticsusa',
    '@Heyonlinemarket'
]

# === 📂 Create base image folder ===
os.makedirs("downloaded_images", exist_ok=True)

# === 📦 List to hold all post results ===
all_results = []

def clean_text(text):
    """Clean and normalize text"""
    return ' '.join(text.replace('\xa0', ' ').split())

def extract_info(text):
    """Extract structured information from various message formats"""
    text = clean_text(text)
    
    # Extract title (first line or before first emoji/price)
    title_match = re.split(r'\n|💸|☘️☘️PRICE|Price\s*:|💵', text)[0].strip()
    title = title_match[:100] if title_match else "No Title"
    
    # Extract phone numbers (Ethiopian format with +251 or 09)
    phone_matches = re.findall(r'(\+251\d{8,9}|09\d{8})', text)
    phone = phone_matches[0] if phone_matches else ""
    
    # Extract price (handles various formats)
    price_match = re.search(
        r'(Price|💸|☘️☘️PRICE)[:\s]*([\d,]+)|([\d,]+)\s*(ETB|Birr|birr|💵)', 
        text, 
        re.IGNORECASE
    )
    price = ""
    if price_match:
        price = price_match.group(2) or price_match.group(3) or ""
        price = price.replace(',', '').strip()
    
    # Extract location (after location indicators)
    location_match = re.search(
        r'(📍|Address|Location|🌺🌺)[:\s]*(.+?)(?=\n|☘️|📞|@|$)', 
        text, 
        re.IGNORECASE
    )
    location = location_match.group(2).strip() if location_match else ""
    
    # Extract channel mentions
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

# === 🛠️ Start scraping ===
with TelegramClient('anon', api_id, api_hash) as client:
    for channel in channels:
        print(f"📡 Scraping channel: {channel}")
        safe_channel = channel.replace("@", "")
        channel_folder = os.path.join("downloaded_images", safe_channel)
        os.makedirs(channel_folder, exist_ok=True)

        for message in client.iter_messages(channel, limit=200):
            if not message.text:
                continue
                
            info = extract_info(message.text)
            
            # Create a safe filename
            raw_title = info["title"] or "untitled"
            safe_filename = re.sub(r'[^\w\-_. ]', '_', raw_title)[:50] + ".jpg"
            image_path = None

            # Download image if available
            if message.media and isinstance(message.media, MessageMediaPhoto):
                try:
                    path = client.download_media(
                        message.media, 
                        file=os.path.join(channel_folder, safe_filename)
                    )
                    image_path = path.replace('\\', '/')  # Normalize path
                except Exception as e:
                    print(f"❌ Error downloading image: {e}")
            
            # Prepare final post data
            post_data = {
                "title": info["title"],
                "description": info["description"],
                "price": info["price"],
                "phone": info["phone"],
                "location": info["location"],
                "image": f"downloaded_images/{safe_channel}/{safe_filename}" if image_path else None,
                "date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
                "channel": channel
            }
            
            # If channel was mentioned in the message, use that instead
            if info["channel_mention"] and info["channel_mention"].lower() != channel.lower():
                post_data["channel"] = info["channel_mention"]
            
            all_results.append(post_data)

# === 💾 Save results to JSON ===
with open("all_channel_posts.json", "w", encoding="utf-8") as f:
    json.dump(all_results, f, indent=2, ensure_ascii=False)

print(f"\n✅ Done. Scraped {len(all_results)} posts from {len(channels)} channels.")
print("📁 Data saved to all_channel_posts.json and images downloaded to /downloaded_images/")