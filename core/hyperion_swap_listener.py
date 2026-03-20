import asyncio
import grpc
import json
import time
import threading
import requests
from typing import Any, Callable, Dict, Optional
from google.protobuf.json_format import MessageToDict

from config.settings import settings
from utils.logger import get_logger, log_price_update
from core.hyperion_math import decode_sqrt_price, calculate_amount_out

logger = get_logger()

class HyperionSwapListener:
    """Listen to Hyperion SwapEventV3 via Aptos indexer gRPC."""

    # AMI/APT pool configuration for Hyperion
    HYPERION_POOL = "0x617a777d6a19da5bf346af49a7f648acce66db9dd3f98c78bd10ed556708a7da"
    HYPERION_MODULE = "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c"
    SWAP_EVENT_TYPE = f"{HYPERION_MODULE}::pool_v3::SwapEventV3"
    
    AMI_DECIMALS = 8
    APT_DECIMALS = 8
    
    HYPERION_FEE = 0.001  # 0.1% as verified on-chain (Pool 0x617a77 uses Tier 100 but charges 0.1%)

    def __init__(
        self,
        on_swap_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.endpoint = settings.cellana_grpc_endpoint  # Reuse same indexer endpoint
        self.api_key = settings.cellana_grpc_api_key
        self.on_swap_event = on_swap_event
        self.loop = loop or asyncio.get_event_loop()
        
        # Shared state
        self.current_sqrt_price: Optional[int] = None
        self.current_liquidity: Optional[int] = None
        self.last_update_ts: float = 0.0
        self.is_grpc_active: bool = False

    def _metadata(self) -> list[tuple[str, str]]:
        if not self.api_key:
            return []
        return [("authorization", f"Bearer {self.api_key}")]

    def _bootstrap_pool_state(self, silent: bool = False) -> Optional[Dict[str, Any]]:
        """Fetch current pool state (sqrt_price, liquidity) via Aptos REST API."""
        try:
            if not silent:
                logger.info("🔄 Bootstrapping initial pool state from Hyperion...")
            
            rest_api_url = "https://fullnode.mainnet.aptoslabs.com/v1"
            
            # Query pool state resource
            pool_resource_type = f"{self.HYPERION_MODULE}::pool_v3::LiquidityPoolV3"
            
            response = requests.get(
                f"{rest_api_url}/accounts/{self.HYPERION_POOL}/resource/{pool_resource_type}",
                timeout=10
            )
            response.raise_for_status()
            
            resource_data = response.json().get("data", {})
            sqrt_price = resource_data.get("sqrt_price")
            liquidity = resource_data.get("liquidity")
            
            if sqrt_price and liquidity:
                self.current_sqrt_price = int(sqrt_price)
                self.current_liquidity = int(liquidity)
                self.last_update_ts = time.time()
                
                if not silent:
                    logger.success(
                        f"✅ [HYPERION BOOTSTRAP] AMI/APT Pool Initialized\n"
                        f"   sqrt_price={self.current_sqrt_price}\n"
                        f"   liquidity={self.current_liquidity}"
                    )
                
                # Return payload
                return {
                    "type": "hyperion_bootstrap",
                    "pool": self.HYPERION_POOL,
                    "sqrt_price_x64": int(sqrt_price),
                    "liquidity": int(liquidity),
                    "source": "bootstrap"
                }
        except Exception as e:
            if not silent:
                logger.error(f"❌ Error bootstrapping Hyperion reserves: {e}")
            return None
        return None

    async def run_reserve_poll_loop(self) -> None:
        """Async loop that polls pool state via REST every N seconds."""
        base_interval = settings.reserve_poll_interval_s
        logger.info(f"🔄 [HYPERION POLL] Starting pool state polling (every {base_interval:.1f}s)")
        
        consecutive_errors = 0
        while True:
            try:
                now = time.time()
                threshold = max(base_interval * 2, 20.0)
                
                if now - self.last_update_ts > threshold:
                    ok = await asyncio.get_event_loop().run_in_executor(None, self._bootstrap_pool_state, True)
                    if ok:
                        consecutive_errors = 0
                    else:
                        consecutive_errors += 1
                else:
                    consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
            
            wait_time = min(base_interval * (2 ** min(consecutive_errors, 4)), 30.0) if consecutive_errors > 0 else base_interval
            await asyncio.sleep(wait_time)

    async def run(self) -> None:
        """Connect and stream swap events forever."""
        try:
            from aptos.indexer.v1 import raw_data_pb2
            from aptos.indexer.v1 import raw_data_pb2_grpc
        except Exception as e:
            logger.error(f"HyperionSwapListener: missing proto dependencies: {e}")
            return

        # Bootstrap 
        bootstrap_payload = await asyncio.get_event_loop().run_in_executor(None, self._bootstrap_pool_state)
        if bootstrap_payload and self.on_swap_event:
            if asyncio.iscoroutinefunction(self.on_swap_event):
                await self.on_swap_event(bootstrap_payload)
            else:
                self.on_swap_event(bootstrap_payload)

        # Start backup poll
        asyncio.create_task(self.run_reserve_poll_loop(), name="hyperion_poll")

        event_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        loop = asyncio.get_event_loop()
        stop_event = threading.Event()
        
        starting_version = 0
        
        def _grpc_thread():
            nonlocal starting_version
            retry_delay = 5
            
            while not stop_event.is_set():
                channel = None
                try:
                    metadata = self._metadata() + [("x-aptos-request-name", "hyperion-swap-listener")]
                    logger.info(f"🔗 [Hyperion] (Re)connecting gRPC stream (from v={starting_version or 'latest'})...")
                    channel = grpc.secure_channel(
                        self.endpoint,
                        grpc.ssl_channel_credentials(),
                        options=[
                            ("grpc.max_receive_message_length", -1),
                            ("grpc.keepalive_time_ms", 30000),
                            ("grpc.keepalive_timeout_ms", 10000),
                            ("grpc.enable_retries", 1),
                            ("grpc.tcp_nodelay", 1),
                        ]
                    )
                    stub = raw_data_pb2_grpc.RawDataStub(channel)
                    
                    req_version = starting_version
                    if not req_version:
                        ledger_resp = requests.get("https://fullnode.mainnet.aptoslabs.com/v1").json()
                        req_version = int(ledger_resp.get("ledger_version", 0)) - 1000
                        starting_version = req_version
                    
                    stream = stub.GetTransactions(raw_data_pb2.GetTransactionsRequest(starting_version=req_version), metadata=metadata)
                    
                    retry_delay = 5 # Reset 
                    for response in stream:
                        if stop_event.is_set(): break
                        
                        latest_event: Optional[Dict[str, Any]] = None
                        for tx in response.transactions:
                            starting_version = tx.version # Save checkpoint
                            
                            if not hasattr(tx, 'user') or not tx.user: continue
                            
                            has_events = any(hasattr(ev, 'type_str') for ev in getattr(tx.user, 'events', []))
                            if has_events and tx.version % 1000 == 0:
                                logger.info(f"📦 [Hyperion Scan] Online at v={tx.version}")
                                self.is_grpc_active = True

                            for ev in getattr(tx.user, 'events', []):
                                ev_type = getattr(ev, "type_str", "")
                                if ev_type == self.SWAP_EVENT_TYPE:
                                    data = json.loads(ev.data) if isinstance(ev.data, str) else MessageToDict(ev.data)
                                    if data.get("pool") == self.HYPERION_POOL:
                                        latest_event = {
                                            "version": tx.version,
                                            "data": data,
                                            "type": ev_type
                                        }
                        
                        if latest_event:
                            loop.call_soon_threadsafe(event_queue.put_nowait, latest_event)
                            
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                        logger.warning(f"⚠️ [Hyperion] Stream duration limit reached. Reconnecting in {retry_delay}s...")
                    else:
                        logger.warning(f"⚠️ [Hyperion] gRPC status error: {e.code()} - {e.details()}. Retry in {retry_delay}s...")
                except Exception as e:
                    logger.warning(f"❌ [Hyperion] Stream error: {e}. Retry in {retry_delay}s...")
                finally:
                    if channel: channel.close()
                    self.is_grpc_active = False
                    
                if not stop_event.is_set():
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 60)
            
            loop.call_soon_threadsafe(event_queue.put_nowait, None)

        thread = threading.Thread(target=_grpc_thread, daemon=True)
        thread.start()

        while True:
            msg = await event_queue.get()
            if msg is None: break
            
            data = msg["data"]
            ver = msg["version"]
            
            sqrt_price = data.get("sqrt_price") or data.get("sqrt_price_x96") or data.get("sqrt_price_x64")
            liquidity = data.get("liquidity")
            
            if sqrt_price is not None:
                self.current_sqrt_price = int(sqrt_price)
                self.last_update_ts = time.time()
            if liquidity is not None:
                self.current_liquidity = int(liquidity)
                self.last_update_ts = time.time()
            
            if self.current_sqrt_price and self.current_liquidity:
                out, impact = calculate_amount_out(
                    self.current_sqrt_price,
                    self.current_liquidity,
                    100.0,
                    fee_rate=self.HYPERION_FEE,
                    is_token0_to_token1=True # APT -> AMI
                )
                
                price_spot = float(decode_sqrt_price(self.current_sqrt_price)**2)
                
                logger.info(
                    f"⚡ [HYPERION SWAP] v={ver} | Spot={price_spot:.8f} | "
                    f"APT/AMI Swap 100 APT -> {out:.2f} AMI"
                )
                
                if self.on_swap_event:
                    try:
                        payload = {
                            "type": "hyperion_swap",
                            "version": ver,
                            "source": "hyperion",
                            "pool": self.HYPERION_POOL,
                            "sqrt_price_x64": self.current_sqrt_price,
                            "liquidity": self.current_liquidity,
                            "price_spot": price_spot,
                            "est_out_100_apt": out
                        }
                        if asyncio.iscoroutinefunction(self.on_swap_event):
                            asyncio.create_task(self.on_swap_event(payload))
                        else:
                            self.on_swap_event(payload)
                    except Exception as e:
                        logger.error(f"Hyperion callback error: {e}")
