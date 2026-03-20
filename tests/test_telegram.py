import asyncio
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.telegram_notifier import notifier

async def test_telegram():
    print("Testing Telegram Notification...")
    if not notifier.enabled:
        print("❌ Telegram NOT enabled. Please check .env (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID).")
        return

    test_message = (
        "<b>🤖 Arb-Bot Test Notification</b>\n"
        "Status: Online 🟢\n"
        "Message: If you see this, your Telegram Bot integration is working correctly!"
    )
    
    await notifier.send_message(test_message)
    print("✅ Test message sent. Please check your Telegram group/chat.")

if __name__ == "__main__":
    asyncio.run(test_telegram())
