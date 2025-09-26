import asyncio
from telethon import TelegramClient

async def create_simple_session():
    """Simple one-time session creation"""
    session_name = input("Enter session name (default: 'user_session'): ").strip()
    if not session_name:
        session_name = "user_session"
    
    API_ID = 24916488
    API_HASH = "3b7788498c56da1a02e904ff8e92d494"
    
    client = None
    try:
        client = TelegramClient(f"{session_name}.session", API_ID, API_HASH)
        await client.start()
        
        me = await client.get_me()
        print(f"\n✅ Success! Session file created: {session_name}.session")
        print(f"📱 Logged in as: {me.first_name} (@{me.username})")
        
        await client.disconnect()
        print("🔌 Disconnected. Session is ready to use!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if client:
            await client.disconnect()

# Run the simple version
if __name__ == "__main__":
    asyncio.run(create_simple_session())