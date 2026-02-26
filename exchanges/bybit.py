import asyncio
import json
from typing import List
import websockets

from config.settings import settings
from core.price_collector import PriceCollector
from utils.logger import get_logger

logger = get_logger()

BYBIT_WS = settings.bybit_ws_url
MAX_RECONNECT_DELAY = 60  # seconds


class BybitWS:
    """Subscribe to multiple symbols on a single Bybit WebSocket connection."""

    def __init__(
        self,
        collector: PriceCollector,
        symbols: List[str] | str | None = None,
    ):
        self.collector = collector
        if symbols is None:
            self.symbols = [settings.cex_symbol]
        elif isinstance(symbols, str):
            self.symbols = [symbols]
        else:
            self.symbols = list(symbols)

    async def connect(self) -> None:
        reconnect_delay = 1
        while True:
            try:
                logger.info(f"Bybit WS connecting for {self.symbols}...")
                async with websockets.connect(
                    BYBIT_WS, open_timeout=20, ping_interval=20
                ) as ws:
                    reconnect_delay = 1  # reset on successful connect

                    # Subscribe to all symbols in a single message
                    args = [f"orderbook.1.{s}" for s in self.symbols]
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    logger.info(f"Bybit WS subscribed to {args}")

                    async for raw in ws:
                        msg = json.loads(raw)
                        if "data" not in msg:
                            continue

                        data   = msg["data"]
                        # topic = "orderbook.1.APTUSDT" → symbol = "APTUSDT"
                        topic  = msg.get("topic", "")
                        symbol = topic.split(".")[-1] if topic else ""

                        bids = data.get("b", [])
                        asks = data.get("a", [])
                        if bids and asks and symbol:
                            bid     = float(bids[0][0])
                            ask     = float(asks[0][0])
                            bid_qty = float(bids[0][1])
                            ask_qty = float(asks[0][1])
                            self.collector.update(
                                "bybit",
                                symbol,
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
