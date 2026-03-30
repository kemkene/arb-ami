import aiohttp
import asyncio
from typing import Optional
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

class TelegramNotifier:
    """Send notifications to a Telegram chat via Bot API."""

    def __init__(self):
        self.token = settings.telegram_bot_token
        self.chat_id = settings.telegram_chat_id
        self.enabled = bool(self.token and self.chat_id)
        
        if not self.enabled:
            logger.warning("TelegramNotifier: Not enabled (missing token or chat_id)")
        else:
            logger.info(f"TelegramNotifier: Initialized for chat_id {self.chat_id}")

    async def send_message(self, text: str):
        """Send a text message to the configured Telegram chat."""
        if not self.enabled:
            return

        # Telegram limit is 4096 characters. Truncate to be safe.
        max_len = 4000
        if len(text) > max_len:
            logger.warning(f"TelegramNotifier: Truncating message from {len(text)} to {max_len} characters")
            text = text[:max_len] + "\n... (truncated)"

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as response:
                    if response.status != 200:
                        resp_text = await response.text()
                        logger.error(f"TelegramNotifier: Failed to send message (status {response.status}, len {len(text)}): {resp_text}")
                    else:
                        logger.debug(f"TelegramNotifier: Message sent successfully (len {len(text)})")
        except Exception as e:
            logger.error(f"TelegramNotifier: Error sending message: {e}")

    async def set_commands(self):
        """Register /status and /stop commands with Telegram."""
        if not self.enabled:
            return

        url = f"https://api.telegram.org/bot{self.token}/setMyCommands"
        payload = {
            "commands": [
                {"command": "status", "description": "Kiểm tra số dư và tình trạng bot"},
                {"command": "stop", "description": "Dừng bot an toàn từ xa"}
            ]
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as response:
                    if response.status == 200:
                        logger.info("TelegramNotifier: Commands registered successfully")
                    else:
                        resp_text = await response.text()
                        logger.error(f"TelegramNotifier: Failed to set commands: {resp_text}")
        except Exception as e:
            logger.error(f"TelegramNotifier: Error setting commands: {e}")

    async def delete_webhook(self):
        """Delete any existing webhook to allow getUpdates (polling)."""
        if not self.enabled:
            return

        url = f"https://api.telegram.org/bot{self.token}/deleteWebhook"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        logger.info("TelegramNotifier: Webhook deleted/cleared successfully")
                    else:
                        resp_text = await response.text()
                        logger.warning(f"TelegramNotifier: Failed to delete webhook: {resp_text}")
        except Exception as e:
            logger.error(f"TelegramNotifier: Error deleting webhook: {e}")

    async def get_updates(self, offset: Optional[int] = None, limit: int = 100) -> list:
        """Fetch updates from Telegram Bot API."""
        if not self.enabled:
            return []

        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        params = {"timeout": 30, "limit": limit}
        if offset:
            params["offset"] = offset

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=35) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("result", [])
                    elif response.status == 409:
                        # Conflict usually means another bot instance is running
                        logger.warning("TelegramNotifier: Conflict (409). Another instance is using this token. PLEASE CHECK FOR RUNNING PROCESSES (pkill -f main.py). Retrying in 15s...")
                        await asyncio.sleep(15)
                        return []
                    else:
                        logger.error(f"TelegramNotifier: Failed to get updates (status {response.status})")
        except Exception as e:
            logger.error(f"TelegramNotifier: Error getting updates: {e}")
        return []

    async def run_listener(self, shutdown_event: asyncio.Event, context: Optional[dict] = None):
        """Poll for Telegram commands in a loop."""
        if not self.enabled:
            return

        logger.info("TelegramNotifier: Starting command listener...")
        await self.delete_webhook()
        await self.set_commands()
        
        # Flush old messages: Get the latest update_id and start from there
        logger.info("TelegramNotifier: Flushing old messages...")
        try:
            updates = await self.get_updates(limit=1)
            if updates:
                offset = updates[-1]["update_id"] + 1
                logger.info(f"TelegramNotifier: Skipped old messages, starting from offset {offset}")
            else:
                offset = None
        except Exception as e:
            logger.error(f"TelegramNotifier: Error flushing messages: {e}")
            offset = None
        
        while not shutdown_event.is_set():
            try:
                updates = await self.get_updates(offset)
                for update in updates:
                    offset = update["update_id"] + 1
                    
                    message = update.get("message", {})
                    raw_text = message.get("text", "")
                    if not raw_text:
                        continue
                        
                    # Clean text: remove whitespace and convert to lower
                    text = str(raw_text).strip().lower()
                    from_id = message.get("from", {}).get("id")
                    
                    # Log received message ALWAYS for debugging (at INFO level temporarily)
                    logger.info(f"TelegramNotifier: Received message content: '{text}' from {from_id}")

                    # Basic auth
                    if str(from_id) != str(self.chat_id) and str(message.get("chat", {}).get("id")) != str(self.chat_id):
                        logger.warning(f"TelegramNotifier: Unauthorized access attempt from {from_id}")
                        continue

                    # Improved Command Matching
                    is_status = text.startswith("/status")
                    is_stop = text.startswith("/stop")

                    if is_stop:
                        logger.info(f"TelegramNotifier: Processing /stop command")
                        await self.send_message("🛑 <b>Dừng bot theo yêu cầu...</b>")
                        shutdown_event.set()
                        return
                    elif is_status:
                        logger.info(f"TelegramNotifier: Processing /status command")
                        status_msg = "🟢 <b>Bot đang chạy bình thường.</b>\n"
                        
                        if context:
                            executor = context.get("executor") or context.get("trade_executor")
                            bm = getattr(executor, "balance_manager", None) if executor else None
                            
                            if bm:
                                # 1. Current Balances
                                balance_report = await self._get_balance_report(context)
                                status_msg += f"\n📊 <b>Số dư hiện tại:</b>\n{balance_report}"
                                
                                # 2. Profit Analysis (Simple version for /status)
                                # Note: Actual equity calculation needs live prices.
                                # For /status, we'll just show the base balances.
                            else:
                                balance_report = await self._get_balance_report(context)
                                status_msg += f"\n📊 <b>Số dư hiện tại:</b>\n{balance_report}"
                        
                        await self.send_message(status_msg)

            except Exception as e:
                logger.error(f"TelegramNotifier: Listener loop error: {e}")
                await asyncio.sleep(5)
            
            await asyncio.sleep(1)

    async def _get_balance_report(self, context: dict) -> str:
        """Fetch and format balances using BalanceManager if available."""
        executor = context.get("executor") or context.get("trade_executor")
        bm = getattr(executor, "balance_manager", None) if executor else None
        
        if not bm:
            return "<i>Lỗi: Không tìm thấy BalanceManager.</i>"

        await bm.ensure_fresh()
        report = []
        
        # Exchanges to report
        for exch in ["bybit", "mexc", "dex"]:
            usdt = bm.get_free(exch, "USDT")
            ami = bm.get_free(exch, "AMI")
            apt = bm.get_free(exch, "APT")
            
            if usdt > 0 or ami > 0 or apt > 0:
                label = exch.upper() if exch != "dex" else "🔗 APTOS"
                report.append(f"📦 <b>{label}:</b> {usdt:.2f} USDT | {apt:.4f} APT | {ami:.0f} AMI")
        
        return "\n".join(report) if report else "<i>Không có dữ liệu ví.</i>"

    def send_message_sync(self, text: str):
        """Synchronous wrapper for send_message (uses a new event loop if necessary)."""
        if not self.enabled:
            return
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.send_message(text))
            else:
                loop.run_until_complete(self.send_message(text))
        except Exception as e:
            logger.error(f"TelegramNotifier sync error: {e}")

# Singleton instance
notifier = TelegramNotifier()
