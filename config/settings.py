import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:


    # Token addresses
    ami_token_address: str = field(
        default_factory=lambda: os.getenv(
            "AMI_TOKEN_ADDRESS",
            "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451",
        )
    )
    usdt_token_address: str = field(
        default_factory=lambda: os.getenv(
            "USDT_TOKEN_ADDRESS",
            "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b",
        )
    )
    # Native APT coin address on Aptos
    apt_token_address: str = field(
        default_factory=lambda: os.getenv(
            "APT_TOKEN_ADDRESS",
            "0x1::aptos_coin::AptosCoin",
        )
    )

    # CEX settings
    cex_symbol: str = field(
        default_factory=lambda: os.getenv("CEX_SYMBOL", "AMIUSDT")
    )
    apt_cex_symbol: str = field(
        default_factory=lambda: os.getenv("APT_CEX_SYMBOL", "APTUSDT")
    )
    bybit_ws_url: str = "wss://stream.bybit.com/v5/public/spot"
    mexc_rest_url: str = "https://api.mexc.com/api/v3/ticker/bookTicker"

    # Fees
    bybit_fee: float = field(
        default_factory=lambda: float(os.getenv("BYBIT_FEE", "0.001"))
    )
    mexc_fee: float = field(
        default_factory=lambda: float(os.getenv("MEXC_FEE", "0.001"))
    )


    # Intervals (seconds)
    mexc_poll_interval: float = field(
        default_factory=lambda: float(os.getenv("MEXC_POLL_INTERVAL", "0.4"))
    )
    arb_check_interval: float = field(
        default_factory=lambda: float(os.getenv("ARB_CHECK_INTERVAL", "0.1"))
    )

    # Minimum profit to log
    min_profit_threshold: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_THRESHOLD", "1.0"))
    )

    # ------------------------------------------------------------------ #
    #  Trade execution
    # ------------------------------------------------------------------ #
    # Set DRY_RUN=false in .env to enable live trading
    dry_run: bool = field(
        default_factory=lambda: os.getenv("DRY_RUN", "true").lower() != "false"
    )
    # Max USDT value per trade leg (safety cap)
    trade_amount_usdt: float = field(
        default_factory=lambda: float(os.getenv("TRADE_AMOUNT_USDT", "10.0"))
    )

    # ------------------------------------------------------------------ #
    #  Cellana swap event listener (Aptos fullnode gRPC)
    # ------------------------------------------------------------------ #
    cellana_swap_listener_enabled: bool = field(
        default_factory=lambda: os.getenv("CELLANA_SWAP_LISTENER_ENABLED", "false").lower() == "true"
    )
    cellana_grpc_endpoint: str = field(
        default_factory=lambda: os.getenv("CELLANA_GRPC_ENDPOINT", "127.0.0.1:50051")
    )
    cellana_grpc_use_tls: bool = field(
        default_factory=lambda: os.getenv("CELLANA_GRPC_USE_TLS", "false").lower() == "true"
    )
    cellana_grpc_api_key: str = field(
        default_factory=lambda: os.getenv("CELLANA_GRPC_API_KEY", "")
    )
    cellana_grpc_api_key_header: str = field(
        default_factory=lambda: os.getenv("CELLANA_GRPC_API_KEY_HEADER", "x-api-key")
    )
    cellana_swap_event_type: str = field(
        default_factory=lambda: os.getenv(
            "CELLANA_SWAP_EVENT_TYPE",
            "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::SwapEvent",
        )
    )
    cellana_swap_pool_address: str = field(
        default_factory=lambda: os.getenv("CELLANA_SWAP_POOL_ADDRESS", "")
    )
    cellana_swap_starting_version: int = field(
        default_factory=lambda: int(os.getenv("CELLANA_SWAP_STARTING_VERSION", "0"))
    )



    # Bybit API credentials
    bybit_api_key: str = field(
        default_factory=lambda: os.getenv("BYBIT_API_KEY", "")
    )
    bybit_api_secret: str = field(
        default_factory=lambda: os.getenv("BYBIT_API_SECRET", "")
    )

    # MEXC API credentials
    mexc_api_key: str = field(
        default_factory=lambda: os.getenv("MEXC_API_KEY", "")
    )
    mexc_api_secret: str = field(
        default_factory=lambda: os.getenv("MEXC_API_SECRET", "")
    )




settings = Settings()
