import asyncio
from typing import Any, Callable, Dict, Optional

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()


class CellanaSwapListener:
    """Listen to Cellana swap events via Aptos fullnode gRPC."""

    _AMOUNT_KEYS = (
        ("amount_in", "amount_out"),
        ("amount_in_x", "amount_out_y"),
        ("amount_in_y", "amount_out_x"),
        ("x_in", "y_out"),
        ("y_in", "x_out"),
        ("input_amount", "output_amount"),
        ("amount_in", "amount_out_min"),
    )

    # Cellana uses from_token/to_token; others use coin_in/coin_out
    _TOKEN_IN_KEYS = ("from_token", "coin_in_type", "token_in", "input_coin", "token_in_type", "in_type")
    _TOKEN_OUT_KEYS = ("to_token", "coin_out_type", "token_out", "output_coin", "token_out_type", "out_type")
    _TOKEN_X_KEYS = ("coin_x_type", "x_type", "token_x", "x_coin")
    _TOKEN_Y_KEYS = ("coin_y_type", "y_type", "token_y", "y_coin")

    def __init__(
        self,
        on_swap_event: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.endpoint = settings.cellana_grpc_endpoint
        self.use_tls = settings.cellana_grpc_use_tls
        self.api_key = settings.cellana_grpc_api_key
        self.api_key_header = settings.cellana_grpc_api_key_header
        self.swap_event_type = settings.cellana_swap_event_type
        self.pool_address = settings.cellana_swap_pool_address
        self.starting_version = settings.cellana_swap_starting_version
        self.on_swap_event = on_swap_event

        # Reconnect backoff
        self._backoff_s = 1.0
        self._max_backoff_s = 15.0

    def _metadata(self) -> list[tuple[str, str]]:
        if not self.api_key:
            return []
        return [(self.api_key_header, self.api_key)]

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

    def _parse_swap_event(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Cellana swap event data.
        
        Cellana SwapEvent fields:
        - amount_in: integer (as string)
        - amount_out: integer (as string)
        - from_token: token type address (e.g. "0xb365...")
        - to_token: token type address (e.g. "0x1::aptos_coin::AptosCoin")
        - pool: pool address
        
        Note: trader/user is NOT in the event; it comes from tx.sender (transaction context).
        """
        parsed: Dict[str, Any] = {}
        pool_addr = self._get_first(data, ("pool_address", "pool", "pool_id", "pool_addr"))
        if isinstance(pool_addr, str):
            parsed["pool"] = pool_addr

        amount_in = None
        amount_out = None
        for in_key, out_key in self._AMOUNT_KEYS:
            amount_in = self._as_int(data.get(in_key))
            amount_out = self._as_int(data.get(out_key))
            if amount_in is not None or amount_out is not None:
                break

        if amount_in is None and amount_out is None:
            amount_x = self._as_int(data.get("amount_x"))
            amount_y = self._as_int(data.get("amount_y"))
            if amount_x is not None:
                parsed["amount_x"] = amount_x
            if amount_y is not None:
                parsed["amount_y"] = amount_y
        else:
            parsed["amount_in"] = amount_in
            parsed["amount_out"] = amount_out

        token_in = self._get_first(data, self._TOKEN_IN_KEYS)
        token_out = self._get_first(data, self._TOKEN_OUT_KEYS)
        token_x = self._get_first(data, self._TOKEN_X_KEYS)
        token_y = self._get_first(data, self._TOKEN_Y_KEYS)

        if isinstance(token_in, str):
            parsed["token_in"] = token_in
        if isinstance(token_out, str):
            parsed["token_out"] = token_out
        if isinstance(token_x, str):
            parsed["token_x"] = token_x
        if isinstance(token_y, str):
            parsed["token_y"] = token_y

        # Trader is typically from tx.sender in transaction context, not from event data
        for key in ("trader", "sender", "user", "account"):
            val = self._get_first(data, (key,))
            if isinstance(val, str):
                parsed["trader"] = val
                break

        parsed["raw_keys"] = sorted(list(data.keys()))
        return parsed

    async def run(self) -> None:
        """Connect and stream swap events forever (reconnects on failure)."""
        try:
            import grpc
            from google.protobuf.json_format import MessageToDict
            from aptos.internal.fullnode.v1 import fullnode_data_pb2
            from aptos.internal.fullnode.v1 import fullnode_data_pb2_grpc
        except Exception as e:
            logger.error(
                "CellanaSwapListener: missing gRPC/proto dependencies. "
                "Install grpcio and generate Aptos fullnode stubs. "
                f"Error: {e}"
            )
            return

        while True:
            try:
                if self.use_tls:
                    channel = grpc.aio.secure_channel(self.endpoint, grpc.ssl_channel_credentials())
                else:
                    channel = grpc.aio.insecure_channel(self.endpoint)

                stub = fullnode_data_pb2_grpc.FullnodeDataStub(channel)
                request = fullnode_data_pb2.GetTransactionsFromNodeRequest(
                    starting_version=self.starting_version
                )

                logger.info(
                    "CellanaSwapListener connected | "
                    f"endpoint={self.endpoint} start_version={self.starting_version}"
                )

                stream = stub.GetTransactionsFromNode(request, metadata=self._metadata())
                async for response in stream:
                    txs = getattr(response, "transactions", [])
                    for tx in txs:
                        version = getattr(tx, "version", None)
                        tx_sender = getattr(tx, "sender", None)  # Trader is transaction sender
                        events = getattr(tx, "events", [])
                        for ev in events:
                            ev_type = getattr(ev, "type_str", None) or getattr(ev, "type", None)
                            if ev_type != self.swap_event_type:
                                continue

                            data_dict: Dict[str, Any] = {}
                            try:
                                data_dict = MessageToDict(getattr(ev, "data", None) or ev)
                            except Exception:
                                data_dict = {"raw": str(getattr(ev, "data", ev))}

                            data_dict = self._unwrap_data(data_dict)
                            if not self._match_pool(data_dict):
                                continue

                            parsed = self._parse_swap_event(data_dict)
                            # Add transaction sender as trader (since trader not in event)
                            if tx_sender and "trader" not in parsed:
                                parsed["trader"] = tx_sender
                            
                            payload = {
                                "type": ev_type,
                                "version": version,
                                "sender": tx_sender,
                                "data": data_dict,
                                "parsed": parsed,
                            }
                            logger.info(
                                f"[CELLANA SWAP] v={version} sender={tx_sender} "
                                f"amount_in={parsed.get('amount_in')} "
                                f"amount_out={parsed.get('amount_out')} "
                                f"from={parsed.get('token_in')} to={parsed.get('token_out')}"
                            )
                            if self.on_swap_event:
                                try:
                                    self.on_swap_event(payload)
                                except Exception as cb_err:
                                    logger.warning(f"Cellana swap callback error: {cb_err}")

                        if isinstance(version, int):
                            self.starting_version = max(self.starting_version, version + 1)

                await channel.close()

            except asyncio.CancelledError:
                logger.info("CellanaSwapListener cancelled")
                raise
            except Exception as e:
                logger.warning(f"CellanaSwapListener error: {e}")
                await asyncio.sleep(self._backoff_s)
                self._backoff_s = min(self._backoff_s * 2.0, self._max_backoff_s)
            else:
                self._backoff_s = 1.0
