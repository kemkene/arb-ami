import asyncio
import json
import websockets

from config.settings import settings
from core.price_collector import PriceCollector
from utils.logger import get_logger

logger = get_logger()

BYBIT_WS = settings.bybit_ws_url
MAX_RECONNECT_DELAY = 60  # seconds


class BybitWS:
    def __init__(self, collector: PriceCollector, symbol: str | None = None):
        self.collector = collector
        self.symbol = symbol or settings.cex_symbol

    async def connect(self) -> None:
        reconnect_delay = 1
        while True:
            try:
                logger.info(f"Bybit WS connecting for {self.symbol}...")
                async with websockets.connect(
                    BYBIT_WS, open_timeout=20, ping_interval=20
                ) as ws:
                    reconnect_delay = 1  # reset on successful connect
                    sub_msg = {
                        "op": "subscribe",
                        "args": [f"orderbook.1.{self.symbol}"],
                    }
                    await ws.send(json.dumps(sub_msg))
                    logger.info(f"Bybit WS subscribed to orderbook.1.{self.symbol}")

                    async for raw in ws:
                        msg = json.loads(raw)
                        if "data" in msg:
                            data = msg["data"]
                            bids = data.get("b", [])
                            asks = data.get("a", [])
                            if bids and asks:
                                bid = float(bids[0][0])
                                ask = float(asks[0][0])
                                bid_qty = float(bids[0][1])
                                ask_qty = float(asks[0][1])
                                self.collector.update(
                                    "bybit",
                                    self.symbol,
                                    bid,
                                    ask,
                                    bid_qty=bid_qty,
                                    ask_qty=ask_qty,
                                )

            except websockets.ConnectionClosed as e:
                logger.warning(f"Bybit WS closed: {e}. Reconnecting in {reconnect_delay}s...")
            except Exception as e:
                logger.error(f"Bybit WS error: {e}. Reconnecting in {reconnect_delay}s...")

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY)
