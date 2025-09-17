Telegram Scraper & Forwarder


To run bot 
first cd to the file 
then run this script venv\Scripts\activate
python telegramtest.py


This Python script scrapes Telegram channels, saves posts to JSON (with optional image downloads), and forwards 7-day messages to a target channel, while avoiding duplicates.

Features

Scrapes posts from Telegram channels listed in a MongoDB collection.

Saves 24-hour posts and 7-day posts separately to JSON.

Downloads images from posts to downloaded_images/<channel>/.

Cleans up old images (older than 7 days) for 7-day scrape.

Forwards only new messages from channels to a target channel.

Tracks already-forwarded messages to prevent duplicates.

Requirements

Python 3.10+

MongoDB with a collection containing channel usernames.

Telegram API_ID and API_HASH.

Telegram bot token for forwarding.

Python Libraries
pip install telethon pymongo python-dotenv

Setup

Clone/Download the script to a folder on your machine.

Create a .env file in the same folder with the following variables:

API_ID=YOUR_API_ID
API_HASH=YOUR_API_HASH
BOT_TOKEN=YOUR_BOT_TOKEN
MONGO_URI=YOUR_MONGO_URI


MongoDB Collection Setup

Database: yetal

Collection: yetalcollection

Each document should contain:

{
  "username": "@ChannelUsername"
}


Target channel: Update TARGET_CHANNEL in the script to your channel username.

Running the Script

Run the script with Python:

python telegram_scraper.py


Script Actions:

Scrapes last 24 hours → saves scraped_24h.json.

Scrapes last 7 days → saves scraped_7d.json and cleans images older than 7 days.

Forwards new posts from the last 7 days to TARGET_CHANNEL while avoiding duplicates.

Directory Structure
project-folder/
├─ telegram_scraper.py
├─ .env
├─ downloaded_images/
│  ├─ Channel1/
│  │  ├─ image1.jpg
│  │  └─ ...
│  └─ Channel2/
├─ scraped_24h.json
├─ scraped_7d.json
└─ forwarded_messages.json


forwarded_messages.json: Tracks already-forwarded messages to prevent duplicates.

Notes

Images for 7-day posts older than 7 days are automatically deleted.

JSON files (scraped_24h.json & scraped_7d.json) are fully updated each run, no accumulation.

Forwarding is safe: only new posts are sent.

Optional Enhancements

Forward only posts with images.

Automatically schedule scraping with a cron job or Windows Task Scheduler.

Add logging to track errors or skipped posts.