import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    # Panora DEX
    panora_api_key: str = field(
        default_factory=lambda: os.getenv(
            "PANORA_API_KEY",
            "a4^KV_EaTf4MW#ZdvgGKX#HUD^3IFEAOV_kzpIE^3BQGA8pDnrkT7JcIy#HNlLGi",
        )
    )
    panora_api_url: str = "https://api.panora.exchange/swap"

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

    # CEX settings
    cex_symbol: str = field(
        default_factory=lambda: os.getenv("CEX_SYMBOL", "AMIUSDT")
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
    panora_fee: float = field(
        default_factory=lambda: float(os.getenv("PANORA_FEE", "0.003"))
    )

    # Intervals (seconds)
    panora_poll_interval: float = field(
        default_factory=lambda: float(os.getenv("PANORA_POLL_INTERVAL", "1.33"))
    )
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

    # Aptos / Panora wallet
    aptos_private_key: str = field(
        default_factory=lambda: os.getenv("APTOS_PRIVATE_KEY", "")
    )
    aptos_wallet_address: str = field(
        default_factory=lambda: os.getenv("APTOS_WALLET_ADDRESS", "")
    )
    aptos_node_url: str = field(
        default_factory=lambda: os.getenv(
            "APTOS_NODE_URL", "https://fullnode.mainnet.aptoslabs.com/v1"
        )
    )
    # Max gas for Aptos transactions
    aptos_max_gas: int = field(
        default_factory=lambda: int(os.getenv("APTOS_MAX_GAS", "200000"))
    )


settings = Settings()
