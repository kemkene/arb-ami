import asyncio
import grpc
import json
import time
import datetime
import threading
from typing import Any, Callable, Dict, Optional
from google.protobuf.json_format import MessageToDict

from config.settings import settings
from utils.logger import get_logger, log_price_update

logger = get_logger()


class CellanaSwapListener:
    """Listen to Cellana SyncEvent via Aptos indexer gRPC.
    
    SyncEvent is emitted after each swap with updated pool reserves.
    Based on official Aptos indexer-processors Python example:
    https://github.com/aptos-labs/aptos-indexer-processors/tree/main/python
    """

    # SyncEvent contains pool reserves after swap
    _RESERVE_KEYS = (
        ("reserves_1", "reserves_2"),
        ("reserve_x", "reserve_y"),
        ("reserves_x", "reserves_y"),
        ("reserve_1", "reserve_2"),
    )

    # AMI/APT pool configuration
    AMI_APT_POOL = "0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464"
    AMI_DECIMALS = 8  # AMI token has 8 decimals
    APT_DECIMALS = 8  # APT token has 8 decimals
    
    # Cellana fee structure — loaded from settings at init
    # On-chain swap_fee_bps=10 → 0.1% for volatile pools
    VOLATILE_POOL_FEE = 0.001  # overridden in __init__
    STABLE_POOL_FEE = 0.0004   # overridden in __init__

    def __init__(
        self,
        on_swap_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.endpoint = settings.cellana_grpc_endpoint
        self.use_tls = settings.cellana_grpc_use_tls
        self.api_key = settings.cellana_grpc_api_key
        self.swap_event_type = settings.cellana_swap_event_type
        self.pool_address = settings.cellana_swap_pool_address
        self.starting_version = settings.cellana_swap_starting_version
        self.on_swap_event = on_swap_event
        self.loop = loop or asyncio.get_event_loop()

        # Override class-level fees from settings (single source of truth)
        self.VOLATILE_POOL_FEE = settings.cellana_volatile_fee
        self.STABLE_POOL_FEE = settings.cellana_stable_fee

        # Reconnect backoff
        self._backoff_s = 10.0
        self._max_backoff_s = None  # No max cap
        
        # Shared state
        self.last_update_ts: float = 0.0
        self.is_grpc_active: bool = False

    def _metadata(self) -> list[tuple[str, str]]:
        """Build gRPC metadata with Bearer authentication (Aptos standard)."""
        if not self.api_key:
            return []
        return [("authorization", f"Bearer {self.api_key}")]

    def _match_pool(self, data: Dict[str, Any]) -> bool:
        if not self.pool_address:
            return True
        for key in ("pool_address", "pool", "pool_id", "pool_addr", "pool_address_hex"):
            val = data.get(key)
            if isinstance(val, str) and val.lower() == self.pool_address.lower():
                return True
        return False

    def _unwrap_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(data.get("data"), dict):
            return data.get("data")
        return data

    def _as_int(self, val: Any) -> Optional[int]:
        if val is None:
            return None
        if isinstance(val, int):
            return val
        if isinstance(val, float):
            return int(val)
        if isinstance(val, str):
            return int(val) if val.isdigit() else None
        if isinstance(val, dict):
            for key in ("value", "amount", "u64", "u128"):
                if key in val:
                    return self._as_int(val.get(key))
        return None

    def _get_first(self, data: Dict[str, Any], keys: tuple[str, ...]) -> Optional[Any]:
        for k in keys:
            if k in data:
                return data.get(k)
        lower = {str(k).lower(): v for k, v in data.items()}
        for k in keys:
            if k.lower() in lower:
                return lower.get(k.lower())
        return None

    def _calculate_price(self, reserves_1: int, reserves_2: int, pool: str, include_fee: bool = False) -> Optional[float]:
        """Calculate token price from pool reserves.
        
        For volatile pools (AMI/APT), uses constant product formula: x * y = k
        Price calculation:
        - Spot price (no fee): reserves_2 / reserves_1
        - Effective price (with fee): adjusts for trading fee impact
        
        For AMI/APT pool (0x4a34...):
        - reserves_1 = AMI amount (8 decimals)
        - reserves_2 = APT amount (8 decimals)
        - Returns: AMI price in APT (e.g., 1 AMI = X APT)
        
        Args:
            reserves_1: Reserve amount of token 1
            reserves_2: Reserve amount of token 2
            pool: Pool address
            include_fee: If True, adjust price for trading fee
        """
        if not reserves_1 or not reserves_2 or reserves_1 <= 0 or reserves_2 <= 0:
            return None
        
        # Calculate spot price (reserves_2 / reserves_1)
        spot_price = float(reserves_2) / float(reserves_1)
        
        # For AMI/APT pool: determine if it's volatile or stable
        fee = self.VOLATILE_POOL_FEE
        if pool and pool.lower() == self.AMI_APT_POOL.lower():
            fee = self.VOLATILE_POOL_FEE  # AMI/APT is a volatile pool
        
        # If including fee, adjust the effective price
        # When buying token1 (paying token2), you pay fee on output
        # Effective price = spot_price * (1 + fee)
        if include_fee:
            return spot_price * (1.0 + fee)
        
        return spot_price

    def _parse_sync_event(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Cellana SyncEvent data.
        
        SyncEvent fields:
        - pool: pool address
        - reserves_1: reserve amount for token 1 (integer as string)
        - reserves_2: reserve amount for token 2 (integer as string)
        
        SyncEvent is emitted after each swap with the updated pool state.
        """
        parsed: Dict[str, Any] = {}
        pool_addr = self._get_first(data, ("pool_address", "pool", "pool_id", "pool_addr"))
        if isinstance(pool_addr, str):
            parsed["pool"] = pool_addr

        # Try different reserve key patterns
        reserve_1 = None
        reserve_2 = None
        for key_1, key_2 in self._RESERVE_KEYS:
            reserve_1 = self._as_int(data.get(key_1))
            reserve_2 = self._as_int(data.get(key_2))
            if reserve_1 is not None or reserve_2 is not None:
                break

        if reserve_1 is not None:
            parsed["reserves_1"] = reserve_1
        if reserve_2 is not None:
            parsed["reserves_2"] = reserve_2

        parsed["raw_keys"] = sorted(list(data.keys()))
        return parsed

    def _get_latest_version(self) -> int:
        """Query latest transaction version from Aptos REST API.
        
        Returns the version of the most recent transaction on-chain.
        Used when starting_version=0 to begin streaming from latest.
        """
        try:
            import requests
            
            logger.info("🔍 Querying latest transaction version from blockchain...")
            
            # Use Aptos public REST API to get ledger info
            # Aptos mainnet REST API: https://fullnode.mainnet.aptoslabs.com/v1
            rest_api_url = "https://fullnode.mainnet.aptoslabs.com/v1"
            
            response = requests.get(rest_api_url, timeout=10)
            response.raise_for_status()
            
            ledger_info = response.json()
            latest_version = int(ledger_info.get("ledger_version", 0))
            
            if latest_version > 0:
                logger.info(f"✅ Latest blockchain version: {latest_version:,}")
                return latest_version
            else:
                logger.warning("⚠️ Could not parse ledger_version from API response")
                return 0
            
        except Exception as e:
            logger.error(f"❌ Error fetching latest version: {e}")
            return 0

    def _bootstrap_pool_reserves(self, silent: bool = False) -> Optional[Dict[str, Any]]:
        """Bootstrap current pool reserves from Cellana at startup.
        
        Queries pool_reserves view function to get initial AMI and APT reserves
        without waiting for SyncEvent. This ensures the arbitrage engine has
        price data immediately when starting.
        
        View function: 
        liquidity_pool::pool_reserves<CoinType_1, CoinType_2>(pool_id)
        
        For AMI/APT pool:
        - Module: 0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool
        - Function: pool_reserves
        - Generic params: 
          T0: 0x1::object::ObjectCore (pool_id type)
          T1: AMI token type
          T2: APT token type
        
        Returns:
            True if successfully initialized, False otherwise
        """
        try:
            import requests
            
            if not silent:
                logger.info("🔄 Bootstrapping initial pool reserves from Cellana...")
            
            rest_api_url = "https://fullnode.mainnet.aptoslabs.com/v1"
            
            # Query pool_reserves view function
            # Module: 0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool
            # Function: pool_reserves<T0: ObjectCore>(pool_id: address) -> (u128, u128)
            cellana_module = "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1"
            liquidity_pool_module = f"{cellana_module}::liquidity_pool"
            
            # Generic type parameter: T0 = ObjectCore (for pool_id type constraint)
            object_core_type = "0x1::object::ObjectCore"
            
            # Pool ID argument
            pool_id = self.AMI_APT_POOL
            
            view_request = {
                "function": f"{liquidity_pool_module}::pool_reserves",
                "type_arguments": [object_core_type],
                "arguments": [pool_id],
            }
            
            logger.debug(f"View request: {json.dumps(view_request, indent=2)}")
            
            # Call view function via REST API
            response = requests.post(
                f"{rest_api_url}/view",
                json=view_request,
                timeout=10
            )
            response.raise_for_status()
            
            result = response.json()
            
            # View function returns a tuple: (reserve_1, reserve_2)
            # For AMI/APT pool: (AMI reserves, APT reserves)
            if isinstance(result, list) and len(result) >= 2:
                reserves_ami = int(result[0])
                reserves_apt = int(result[1])
                
                # Create synthetic SyncEvent payload
                payload = {
                    "type": "pool_reserves_bootstrap",
                    "pool": self.AMI_APT_POOL,
                    "parsed": {
                        "reserves_1": reserves_ami,
                        "reserves_2": reserves_apt,
                    },
                    "timestamp": datetime.datetime.now().isoformat(),
                    "source": "bootstrap"
                }
                
                # Calculate price from reserves
                price_spot = self._calculate_price(reserves_ami, reserves_apt, self.AMI_APT_POOL, include_fee=False)
                price_with_fee = self._calculate_price(reserves_ami, reserves_apt, self.AMI_APT_POOL, include_fee=True)
                
                if price_spot:
                    self.last_update_ts = time.time()
                    if not silent:
                        logger.success(
                            f"✅ [BOOTSTRAP] AMI/APT Pool Initialized\n"
                            f"   Reserves: AMI={reserves_ami:,} | APT={reserves_apt:,}\n"
                            f"   Price (spot): {price_spot:.8f} AMI/APT\n"
                            f"   Price (+fee): {price_with_fee:.8f} AMI/APT"
                        )
                    
                    # Return payload to be handled in async run()
                    return {
                        "type": "pool_reserves_bootstrap",
                        "pool": self.AMI_APT_POOL,
                        "parsed": {
                            "reserves_1": reserves_ami,
                            "reserves_2": reserves_apt,
                        },
                        "timestamp": datetime.datetime.now().isoformat(),
                        "price_ami_per_apt_spot": price_spot,
                        "price_ami_per_apt_with_fee": price_with_fee,
                        "source": "bootstrap"
                    }
                return None
            return None
        except Exception as e:
            if not silent:
                logger.error(f"❌ Error bootstrapping pool reserves: {e}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error bootstrapping pool reserves: {e}")
            try:
                logger.debug(f"Response status: {response.status_code if 'response' in locals() else 'N/A'}")
                logger.debug(f"Response text: {response.text if 'response' in locals() else 'N/A'}")
            except:
                pass
            import traceback
            logger.debug(traceback.format_exc())
            return False

    # ------------------------------------------------------------------ #
    #  REST-based pool reserve polling (complements gRPC event stream)
    # ------------------------------------------------------------------ #

    def _poll_reserves_rest(self, silent: bool = False) -> bool:
        """Poll current reserves for the AMI/APT pool and emit a synthetic SyncEvent."""
        try:
            return self._bootstrap_pool_reserves(silent=silent)
        except Exception as exc:
            if not silent:
                logger.debug(f"REST reserve poll error: {exc}")
            return False

    async def run_reserve_poll_loop(self) -> None:
        """Async loop that polls pool reserves via REST every N seconds."""
        base_interval = settings.reserve_poll_interval_s
        logger.info(f"🔄 [CELLANA POLL] Starting reserve polling (every {base_interval:.1f}s)")
        
        consecutive_errors = 0
        while True:
            try:
                now = time.time()
                # Only poll if data is stale (gap > 2x interval or 20s)
                threshold = max(base_interval * 2, 20.0)
                
                if now - self.last_update_ts > threshold:
                    if consecutive_errors == 0:
                        logger.info(f"💾 [CELLANA POLL] Data stale ({now - self.last_update_ts:.1f}s), falling back to REST poll...")
                    
                    # Use silent poll for regular loop
                    ok = await asyncio.get_event_loop().run_in_executor(None, self._poll_reserves_rest, True)
                    if ok:
                        consecutive_errors = 0
                    else:
                        consecutive_errors += 1
                else:
                    consecutive_errors = 0 # Data is fresh from gRPC
            except Exception as e:
                consecutive_errors += 1
                if "429" in str(e):
                    logger.warning(f"⚠️ [CELLANA POLL] Rate limited (429). Backing off...")
                else:
                    logger.debug(f"Cellana REST poll error: {e}")
            
            # Backoff logic
            if consecutive_errors > 0:
                wait_time = min(base_interval * (2 ** min(consecutive_errors, 4)), 30.0)
            else:
                wait_time = base_interval
            
            await asyncio.sleep(wait_time)

    async def run(self) -> None:
        """Connect and stream swap events forever (reconnects on failure).
        
        Architecture: gRPC stream runs in a dedicated thread to avoid
        blocking the asyncio event loop. Events are passed back via an
        asyncio.Queue so CEX WebSocket feeds remain responsive.
        """
        try:
            from aptos.indexer.v1 import raw_data_pb2
            from aptos.indexer.v1 import raw_data_pb2_grpc
        except Exception as e:
            logger.error(
                "CellanaSwapListener: missing proto dependencies. "
                "Run: cd /home/truong/Desktop/arbitrage/arb-bot && "
                "python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. "
                "aptos/indexer/v1/raw_data.proto. "
                f"Error: {e}"
            )
            return

        reconnection_retries = 0
        last_event_ts = None
        
        # ┌───────────────────────────────────────────────────────────────────┐
        # │  BOOTSTRAP: Initialize current pool reserves before streaming    │
        # │  This ensures the arbitrage engine has a price immediately.      │
        # └───────────────────────────────────────────────────────────────────┘
        logger.info("🔄 [BOOTSTRAP] Initializing AMI/APT pool reserves...")
        
        # Simple retry loop for bootstrap (handles 429 errors)
        bootstrap_payload = None
        for attempt in range(3):
            bootstrap_payload = await asyncio.get_event_loop().run_in_executor(None, self._bootstrap_pool_reserves)
            if bootstrap_payload:
                break
            if attempt < 2:
                logger.warning(f"⚠️ [BOOTSTRAP] Attempt {attempt+1} failed, retrying in 2s...")
                await asyncio.sleep(2)

        if bootstrap_payload:
            logger.success("✅ [BOOTSTRAP] Success - pool reserves initialized")
            # Trigger callback in async context
            if self.on_swap_event:
                if asyncio.iscoroutinefunction(self.on_swap_event):
                    await self.on_swap_event(bootstrap_payload)
                else:
                    self.on_swap_event(bootstrap_payload)
        else:
            logger.warning("⚠️ [BOOTSTRAP] Failed all attempts - will wait for first SyncEvent")
        
        # Auto-detect latest version if starting_version is 0
        if self.starting_version == 0:
            logger.info("⚙️ Starting version is 0, fetching latest version from blockchain...")
            latest_version = await asyncio.get_event_loop().run_in_executor(
                None, self._get_latest_version
            )
            if latest_version > 0:
                self.starting_version = latest_version
                logger.info(f"🚀 Auto-starting from version: {self.starting_version:,}")
            else:
                logger.warning("⚠️ Could not fetch latest version, starting from 0")

        # Queue for passing parsed events from gRPC thread → async event loop
        event_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        loop = asyncio.get_event_loop()

        while True:
            try:
                # Run blocking gRPC stream in a dedicated thread
                grpc_error_holder: list = []  # [0]=exception if any
                stop_event = threading.Event()

                def _grpc_stream_thread():
                    """Blocking gRPC stream running in a separate thread.
                    
                    Parses SyncEvents and puts payloads into the async queue
                    so the event loop is never blocked.
                    """
                    channel = None
                    try:
                        metadata = [
                            ("authorization", f"Bearer {self.api_key}"),
                            ("x-aptos-request-name", "cellana-swap-listener"),
                        ]
                        options = [
                            ("grpc.max_receive_message_length", -1),
                            ("grpc.keepalive_time_ms", 30000),
                            ("grpc.keepalive_timeout_ms", 10000),
                            ("grpc.enable_retries", 1),
                            ("grpc.tcp_nodelay", 1),
                        ]
                        channel = grpc.secure_channel(
                            self.endpoint,
                            grpc.ssl_channel_credentials(),
                            options=options,
                        )
                        stub = raw_data_pb2_grpc.RawDataStub(channel)
                        request = raw_data_pb2.GetTransactionsRequest(
                            starting_version=self.starting_version,
                        )

                        logger.info(
                            f"CellanaSwapListener connected (threaded) | "
                            f"endpoint={self.endpoint} start_version={self.starting_version}"
                        )
                        self.is_grpc_active = True

                        current_version = self.starting_version
                        stream = stub.GetTransactions(request, metadata=metadata)
                        processed_count = 0
                        status_interval = 1000

                        for response in stream:
                            if stop_event.is_set():
                                break

                            txs = response.transactions
                            if not txs:
                                continue

                            batch_start_version = txs[0].version
                            batch_end_version = txs[-1].version

                            has_events = any(
                                hasattr(tx, 'user') and tx.user and
                                hasattr(tx.user, 'events') and tx.user.events
                                for tx in txs
                            )
                            if has_events:
                                logger.info(
                                    f"📦 Batch v={batch_start_version}-{batch_end_version} "
                                    f"({len(txs)} txs) - contains events"
                                )

                            if current_version != batch_start_version:
                                logger.warning(
                                    f"Version gap detected: expected {current_version}, "
                                    f"got {batch_start_version}"
                                )
                                logger.info(f"[GAP] Detected version gap at {datetime.datetime.now()} (expected={current_version}, got={batch_start_version})")
                                current_version = batch_start_version

                            latest_event_per_pool: Dict[str, Dict[str, Any]] = {}

                            for tx in txs:
                                version = tx.version if hasattr(tx, 'version') else None
                                if not hasattr(tx, 'user') or not tx.user:
                                    continue
                                tx_sender = None
                                if hasattr(tx.user, 'request') and tx.user.request:
                                    tx_sender = getattr(tx.user.request, 'sender', None)
                                events = getattr(tx.user, 'events', [])

                                for event_index, ev in enumerate(events):
                                    ev_type = getattr(ev, "type_str", None)
                                    if not ev_type:
                                        continue
                                        
                                    is_v2_sync = ev_type == self.swap_event_type
                                    
                                    if not is_v2_sync:
                                        # Reduce noise: only log other liquidity events at higher debug level if needed
                                        # logging thousands of events per block slows down the processor
                                        # if version and ("liquidity_pool" in ev_type or "Sync" in ev_type or "swap" in ev_type.lower()):
                                        #     logger.debug(f"  📍 TX v={version} Event[{event_index}]: {ev_type}")
                                        continue

                                    logger.success(
                                        f"  🎯 MATCHED {ev_type} at index {event_index} in tx v={version}"
                                    )

                                    data_dict: Dict[str, Any] = {}
                                    try:
                                        if hasattr(ev, "data") and ev.data:
                                            if isinstance(ev.data, str):
                                                data_dict = json.loads(ev.data)
                                            else:
                                                data_dict = MessageToDict(
                                                    ev.data, preserving_proto_field_name=True
                                                )
                                        else:
                                            logger.debug(f"  No data in event payload")
                                    except json.JSONDecodeError as json_err:
                                        logger.debug(f"Failed to parse event data as JSON: {json_err}")
                                        data_dict = {"raw": str(ev.data)}
                                    except Exception as parse_err:
                                        logger.debug(f"Failed to parse event data: {parse_err}")
                                        data_dict = {"raw": str(getattr(ev, "data", ev))}

                                    data_dict = self._unwrap_data(data_dict)

                                    event_pool = data_dict.get('pool', '')
                                    is_target_pool = event_pool and event_pool.lower() == self.AMI_APT_POOL.lower()
                                    if not is_target_pool:
                                        continue

                                    parsed = self._parse_sync_event(data_dict)
                                    payload = {
                                        "type": ev_type,
                                        "version": version,
                                        "sender": tx_sender,
                                        "data": data_dict,
                                        "parsed": parsed,
                                    }
                                    latest_event_per_pool[event_pool.lower()] = payload

                                if version is not None:
                                    current_version = max(current_version, version + 1)
                                    self.starting_version = current_version
                                    self.is_grpc_active = True

                            if processed_count % 10 == 0 and processed_count > 0:
                                logger.info(
                                    f"✓ Checkpoint: current_version={current_version} "
                                    f"| events_found={processed_count}"
                                )

                            # Push latest event from this batch into the async queue
                            if latest_event_per_pool:
                                processed_count += len(latest_event_per_pool)
                                for payload in latest_event_per_pool.values():
                                    # thread-safe: schedule put on the event loop
                                    loop.call_soon_threadsafe(event_queue.put_nowait, payload)

                            if processed_count > 0 and processed_count % status_interval == 0:
                                logger.info(
                                    f"[CELLANA STATUS] Processed {processed_count} events, "
                                    f"current_version={current_version}"
                                )
                            logger.debug(
                                f"[Parser] Processed batch: {batch_start_version} -> {batch_end_version}"
                            )

                        logger.info("CellanaSwapListener stream ended normally")

                    except grpc.RpcError as e:
                        self.is_grpc_active = False
                        grpc_error_holder.append(e)
                        logger.warning(
                            f"CellanaSwapListener gRPC error in thread: "
                            f"{e.code()} - {e.details()}"
                        )
                    except Exception as e:
                        self.is_grpc_active = False
                        grpc_error_holder.append(e)
                        logger.warning(f"CellanaSwapListener thread error: {e}")
                    finally:
                        if channel:
                            try:
                                channel.close()
                            except Exception:
                                pass
                        # Signal the async consumer that the stream ended
                        loop.call_soon_threadsafe(event_queue.put_nowait, None)

                # Start gRPC in a daemon thread
                grpc_thread = threading.Thread(
                    target=_grpc_stream_thread, daemon=True, name="grpc-cellana"
                )
                grpc_thread.start()

                # Reset reconnection counter (stream started successfully)
                reconnection_retries = 0
                self._backoff_s = 1.0

                # Async consumer: process events from queue WITHOUT blocking event loop
                while True:
                    payload = await event_queue.get()
                    if payload is None:
                        # Stream ended or errored — break to reconnect
                        break

                    version = payload['version']
                    sender = payload['sender']
                    parsed = payload['parsed']
                    reserves_1 = parsed.get('reserves_1')
                    reserves_2 = parsed.get('reserves_2')
                    pool = parsed.get('pool')

                    price_spot = None
                    price_with_fee = None
                    if reserves_1 and reserves_2:
                        price_spot = self._calculate_price(
                            reserves_1, reserves_2, pool, include_fee=False
                        )
                        price_with_fee = self._calculate_price(
                            reserves_1, reserves_2, pool, include_fee=True
                        )
                        if price_spot:
                            parsed['price_ami_per_apt_spot'] = price_spot
                            parsed['price_ami_per_apt_with_fee'] = price_with_fee
                            payload['price_ami_per_apt_spot'] = price_spot
                            payload['price_ami_per_apt_with_fee'] = price_with_fee
                            payload['price_ami_per_apt_with_fee'] = price_with_fee
                            payload['price_ami_per_apt'] = price_spot
                        
                        self.last_update_ts = time.time()

                    # V3 logic
                    if parsed.get("is_v3"):
                        sqrt_p = parsed.get("sqrt_price_x64")
                        if sqrt_p:
                            from core.hyperion_math import decode_sqrt_price
                            # Cellana V3 uses sqrtPriceX64 similar to Hyperion/UniswapV3
                            # price = (sqrtPrice / 2^64)^2
                            price_spot = float(decode_sqrt_price(sqrt_p)**2)
                            # For parity with Hyperion: 
                            # If pool is AMI/APT, decode_sqrt_price might return AMI per APT or vice-versa
                            # depending on token order. 
                            # Let's assume standard order for now or just log it.
                            parsed['price_ami_per_apt_spot'] = price_spot
                            payload['price_ami_per_apt_spot'] = price_spot
                            payload['price_ami_per_apt'] = price_spot

                    price_spot_str = f"{price_spot:.8f}" if price_spot else "None"
                    price_fee_str = f"{price_with_fee:.8f}" if price_with_fee else "None"

                    logger.info(
                        f"💰 [AMI/APT PRICE] "
                        f"v={version} | "
                        f"spot={price_spot_str} | "
                        f"+fee={price_fee_str} | "
                        f"reserves_AMI={reserves_1:,} | "
                        f"reserves_APT={reserves_2:,}"
                    )

                    if price_spot and price_with_fee:
                        log_price_update({
                            "pool": pool,
                            "pool_name": "AMI/APT",
                            "version": int(version) if version else None,
                            "reserves_1": int(reserves_1) if reserves_1 else None,
                            "reserves_2": int(reserves_2) if reserves_2 else None,
                            "reserves_ami": int(reserves_1) if reserves_1 else None,
                            "reserves_apt": int(reserves_2) if reserves_2 else None,
                            "price_spot": float(price_spot),
                            "price_with_fee": float(price_with_fee),
                            "fee_pct": 0.1,
                            "source": "cellana_dex",
                        })

                    if self.on_swap_event:
                        try:
                            if asyncio.iscoroutinefunction(self.on_swap_event):
                                asyncio.create_task(self.on_swap_event(payload))
                            else:
                                self.on_swap_event(payload)
                        except Exception as cb_err:
                            logger.warning(f"Cellana swap callback error: {cb_err}")

                # Clean up thread
                stop_event.set()
                grpc_thread.join(timeout=5)

                # Check if it was an error or normal end
                if grpc_error_holder:
                    self.is_grpc_active = False
                    raise grpc_error_holder[0]

                # Stream ended normally — reconnect
                logger.warning("CellanaSwapListener stream ended normally, reconnecting...")
                logger.info(f"[RECONNECT] Stream ended, reconnecting at {datetime.datetime.now()} (retries={reconnection_retries})")

            except asyncio.CancelledError:
                logger.info("CellanaSwapListener cancelled")
                stop_event.set()
                raise
                
            except grpc.RpcError as e:
                reconnection_retries += 1
                logger.warning(
                    f"CellanaSwapListener gRPC error (retry {reconnection_retries}): "
                    f"{e.code()} - {e.details()}. Retrying in {self._backoff_s}s"
                )
                logger.info(f"[RECONNECT] gRPC error at {datetime.datetime.now()} (retry={reconnection_retries}, backoff={self._backoff_s}s)")
                await asyncio.sleep(self._backoff_s)
                self._backoff_s = self._backoff_s * 2.0
                
            except Exception as e:
                reconnection_retries += 1
                logger.warning(
                    f"CellanaSwapListener error (retry {reconnection_retries}): "
                    f"{e}. Retrying in {self._backoff_s}s"
                )
                logger.info(f"[RECONNECT] General error at {datetime.datetime.now()} (retry={reconnection_retries}, backoff={self._backoff_s}s)")
                await asyncio.sleep(self._backoff_s)
                self._backoff_s = self._backoff_s * 2.0
