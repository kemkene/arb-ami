import os
from dataclasses import dataclass, field
from typing import Optional
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
    mexc_rest_url: str = "https://api.mexc.com"
    mexc_depth_url: str = "https://api.mexc.com/api/v3/depth"

    # Orderbook depth settings
    bybit_orderbook_depth: int = field(
        default_factory=lambda: int(os.getenv("BYBIT_ORDERBOOK_DEPTH", "50"))
    )
    mexc_orderbook_depth: int = field(
        default_factory=lambda: int(os.getenv("MEXC_ORDERBOOK_DEPTH", "20"))
    )

    # Fees
    bybit_fee: float = field(
        default_factory=lambda: float(os.getenv("BYBIT_FEE", "0.001"))
    )
    mexc_fee: float = field(
        default_factory=lambda: float(os.getenv("MEXC_FEE", "0.001"))
    )
    # Cellana DEX fee for volatile pools (AMI/APT)
    # On-chain swap_fee_bps=10 → 10 basis points = 0.1%
    cellana_volatile_fee: float = field(
        default_factory=lambda: float(os.getenv("CELLANA_VOLATILE_FEE", "0.001"))
    )
    # Cellana DEX fee for stable pools (if ever needed)
    cellana_stable_fee: float = field(
        default_factory=lambda: float(os.getenv("CELLANA_STABLE_FEE", "0.0004"))
    )


    # Intervals (seconds)
    mexc_poll_interval: float = field(
        default_factory=lambda: float(os.getenv("MEXC_POLL_INTERVAL", "0.5"))
    )
    arb_check_interval: float = field(
        default_factory=lambda: float(os.getenv("ARB_CHECK_INTERVAL", "0.1"))
    )

    # Profitability Thresholds (in USDT)
    min_profit_threshold: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_THRESHOLD", "0.5")) # Higher threshold to filter small noisy trades
    )

    # ── Enhanced Logging & Diagnostics ─────────────────────────────
    log_shadow_opportunities: bool = field(
        default_factory=lambda: os.getenv("LOG_SHADOW_OPPORTUNITIES", "true").lower() == "true"
    )
    virtual_sizing_enabled: bool = field(
        default_factory=lambda: os.getenv("VIRTUAL_SIZING_ENABLED", "true").lower() == "true"
    )
    min_shadow_profit_usd: float = field(
        default_factory=lambda: float(os.getenv("MIN_SHADOW_PROFIT_USD", "0.01"))
    )

    # Strategy-specific profit thresholds (USD)
    # DEX-involved routes: must cover gas (~$0.005) + slippage + execution risk
    # CEX-CEX routes: lower threshold (no gas, tighter spreads)
    min_profit_dex_to_cex: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_DEX_TO_CEX", "0.05"))
    )
    min_profit_ami_cycle: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_AMI_CYCLE", "0.05"))
    )
    min_profit_cross_cex: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_CROSS_CEX", "0.05"))
    )
    min_profit_apt_cycle: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_APT_CYCLE", "0.05"))
    )
    min_profit_cex_to_cex: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_CEX_TO_CEX", "0.02"))
    )

    # Strategy-specific profit thresholds (percentage) — triggers if BOTH USD AND % met
    min_profit_pct_dex_to_cex: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_PCT_DEX_TO_CEX", "0.05"))
    )
    min_profit_pct_ami_cycle: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_PCT_AMI_CYCLE", "0.05"))
    )
    min_profit_pct_cross_cex: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_PCT_CROSS_CEX", "0.05"))
    )
    min_profit_pct_apt_cycle: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_PCT_APT_CYCLE", "0.05"))
    )
    min_profit_pct_cex_to_cex: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_PCT_CEX_TO_CEX", "0.02"))
    )

    # ── Telegram Notifications ─────────────────────────────────────
    telegram_bot_token: Optional[str] = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN")
    )
    telegram_chat_id: Optional[str] = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID")
    )

    # APT-start cycle thresholds (start with APT, profit in APT)
    min_profit_apt_start: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_APT_START", "0.05"))
    )
    min_profit_pct_apt_start: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_PCT_APT_START", "0.05"))
    )

    # AMI-start cycle thresholds (start with AMI, profit in AMI)
    min_profit_ami_start: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_AMI_START", "0.05"))
    )
    min_profit_pct_ami_start: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_PCT_AMI_START", "0.05"))
    )

    # ── CEX-to-CEX Rebalance Settings ──────────────────────────────
    # Only move surplus if Balance(Exchange A) > Threshold * Factor
    cex_rebalance_threshold_factor: float = field(
        default_factory=lambda: float(os.getenv("CEX_REBALANCE_THRESHOLD_FACTOR", "2.0"))
    )

    # DEX-DEX cycle thresholds
    enable_dex_dex_arb: bool = field(
        default_factory=lambda: os.getenv("ENABLE_DEX_DEX_ARB", "true").lower() == "true"
    )
    min_profit_dex_dex: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_DEX_DEX", "0.05"))
    )
    min_profit_pct_dex_dex: float = field(
        default_factory=lambda: float(os.getenv("MIN_PROFIT_PCT_DEX_DEX", "0.2"))
    )
    dex_dex_gas_fee_apt: float = field(
        default_factory=lambda: float(os.getenv("DEX_DEX_GAS_FEE_APT", "0.01"))
    )

    # ── Fee optimization ────────────────────────────────────────────
    # Use maker fee instead of taker for CEX legs (MEXC maker=0%!)
    # "taker" = market order (guaranteed fill, higher fee)
    # "maker" = limit order (may not fill, lower/zero fee)
    use_maker_fee: bool = field(
        default_factory=lambda: os.getenv("USE_MAKER_FEE", "false").lower() == "true"
    )

    # ── Gas cost subtraction ────────────────────────────────────────
    # Estimated on-chain gas cost per DEX swap (in USD)
    # Aptos typical swap gas: ~0.003-0.005 APT ≈ $0.003-$0.005
    gas_cost_usd: float = field(
        default_factory=lambda: float(os.getenv("GAS_COST_USD", "0.05"))
    )
    # Aptos typical swap gas limit (units)
    swap_gas_limit: int = field(
        default_factory=lambda: int(os.getenv("SWAP_GAS_LIMIT", "10000"))
    )
    cellana_swap_gas_limit: int = field(
        default_factory=lambda: int(os.getenv("CELLANA_SWAP_GAS_LIMIT", "50000"))
    )
    hyperion_swap_gas_limit: int = field(
        default_factory=lambda: int(os.getenv("HYPERION_SWAP_GAS_LIMIT", "80000"))
    )
    # Estimated real gas usage for profit calculation (units)
    estimated_gas_usage: int = field(
        default_factory=lambda: int(os.getenv("ESTIMATED_GAS_USAGE", "18000"))
    )
    # Gas price polling interval
    gas_poll_interval_s: float = field(
        default_factory=lambda: float(os.getenv("GAS_POLL_INTERVAL_S", "60.0"))
    )

    # ── Execution risk buffer ───────────────────────────────────────
    # Safety margin (%) subtracted from detected profit before execution.
    # Accounts for slippage, latency, partial fills, price movement
    # between detection and execution.  Set to 0 to disable.
    execution_risk_buffer_pct: float = field(
        default_factory=lambda: float(os.getenv("EXECUTION_RISK_BUFFER_PCT", "0.1"))
    )

    # DEX execution realism
    dex_block_delay_ms: float = field(
        default_factory=lambda: float(os.getenv("DEX_BLOCK_DELAY_MS", "500"))
    )
    dex_slippage_buffer_pct: float = field(
        default_factory=lambda: float(os.getenv("DEX_SLIPPAGE_BUFFER_PCT", "0.2"))
    )

    # Adaptive slippage (trade-size aware)
    adaptive_slippage_enabled: bool = field(
        default_factory=lambda: os.getenv("ADAPTIVE_SLIPPAGE_ENABLED", "true").lower() == "true"
    )
    # Base slippage (%) applied regardless of trade size
    adaptive_slippage_base_pct: float = field(
        default_factory=lambda: float(os.getenv("ADAPTIVE_SLIPPAGE_BASE_PCT", "0.05"))
    )
    # Multiplier for (trade_size / pool_reserve) ratio
    adaptive_slippage_impact_mult: float = field(
        default_factory=lambda: float(os.getenv("ADAPTIVE_SLIPPAGE_IMPACT_MULT", "0.5"))
    )
    # Maximum adaptive slippage cap (%)
    adaptive_slippage_max_pct: float = field(
        default_factory=lambda: float(os.getenv("ADAPTIVE_SLIPPAGE_MAX_PCT", "3.0"))
    )

    # DEX spike detection threshold (%) for priority arb check
    dex_spike_threshold_pct: float = field(
        default_factory=lambda: float(os.getenv("DEX_SPIKE_THRESHOLD_PCT", "0.3"))
    )
    # Duration (sec) to bypass CEX throttle after a DEX spike
    dex_spike_priority_duration: float = field(
        default_factory=lambda: float(os.getenv("DEX_SPIKE_PRIORITY_DURATION", "2.0"))
    )

    # Balance manager
    balance_refresh_ttl: float = field(
        default_factory=lambda: float(os.getenv("BALANCE_REFRESH_TTL", "10.0"))
    )
    balance_reserve_buffer_pct: float = field(
        default_factory=lambda: float(os.getenv("BALANCE_RESERVE_BUFFER_PCT", "0.02"))
    )

    # De-duplication / cooldown
    arb_dedup_cooldown_sec: float = field(
        default_factory=lambda: float(os.getenv("ARB_DEDUP_COOLDOWN_SEC", "2.0"))
    )
    arb_price_round_decimals: int = field(
        default_factory=lambda: int(os.getenv("ARB_PRICE_ROUND_DECIMALS", "4"))
    )

    # ------------------------------------------------------------------ #
    #  Trade execution
    # ------------------------------------------------------------------ #
    # Set DRY_RUN=false in .env to enable live trading
    dry_run: bool = field(
        default_factory=lambda: os.getenv("DRY_RUN", "true").strip().lower() == "true"
    )
    # Max USDT value per trade leg (safety cap)
    trade_amount_usdt: float = field(
        default_factory=lambda: float(os.getenv("TRADE_AMOUNT_USDT", "500.0"))
    )
    # Max USDT for opportunity detection (can be larger than trade_amount_usdt)
    max_trade_usdt: float = field(
        default_factory=lambda: float(os.getenv("MAX_TRADE_USDT", "500.0"))
    )
    # Min USDT for trade size optimizer lower bound
    min_trade_usdt: float = field(
        default_factory=lambda: float(os.getenv("MIN_TRADE_USDT", "10.0"))
    )
    # Enable automatic optimal trade-size search (golden-section)
    optimal_size_enabled: bool = field(
        default_factory=lambda: os.getenv("OPTIMAL_SIZE_ENABLED", "true").lower() == "true"
    )
    # Number of candidate sizes the optimizer evaluates
    optimal_size_steps: int = field(
        default_factory=lambda: int(os.getenv("OPTIMAL_SIZE_STEPS", "8"))
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
    cellana_swap_event_type: str = field(
        default_factory=lambda: os.getenv(
            "CELLANA_SWAP_EVENT_TYPE",
            "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::SyncEvent",
        )
    )
    cellana_swap_pool_address: str = field(
        default_factory=lambda: os.getenv("CELLANA_SWAP_POOL_ADDRESS", "")
    )
    cellana_swap_starting_version: int = field(
        default_factory=lambda: int(os.getenv("CELLANA_SWAP_STARTING_VERSION", "0"))
    )

    # ------------------------------------------------------------------ #
    #  Hyperion swap event listener (Aptos fullnode gRPC)
    # ------------------------------------------------------------------ #
    hyperion_swap_listener_enabled: bool = field(
        default_factory=lambda: os.getenv("HYPERION_SWAP_LISTENER_ENABLED", "true").lower() == "true"
    )
    hyperion_swap_pool_address: str = field(
        default_factory=lambda: os.getenv(
            "HYPERION_SWAP_POOL_ADDRESS",
            "0x617a777d6a19da5bf346af49a7f648acce66db9dd3f98c78bd10ed556708a7da"
        )
    )
    hyperion_fee_rate: float = field(
        default_factory=lambda: float(os.getenv("HYPERION_FEE_RATE", "0.001"))
    )
    # REST-based pool reserve polling (complements gRPC event stream)
    reserve_poll_interval_s: float = field(
        default_factory=lambda: float(os.getenv("RESERVE_POLL_INTERVAL_S", "15.0"))
    )
    reserve_poll_enabled: bool = field(
        default_factory=lambda: os.getenv("RESERVE_POLL_ENABLED", "true").lower() == "true"
    )
    aptos_rest_url: str = field(
        default_factory=lambda: os.getenv(
            "APTOS_REST_URL", "https://fullnode.mainnet.aptoslabs.com/v1"
        )
    )

    # ------------------------------------------------------------------ #
    #  Aptos on-chain (DEX swap)
    # ------------------------------------------------------------------ #
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
    aptos_node_urls: list[str] = field(
        default_factory=lambda: [
            u.strip() for u in os.getenv(
                "APTOS_NODE_URLS", 
                "https://fullnode.mainnet.aptoslabs.com/v1,https://aptos.nodereal.io/v1/public,https://mainnet.aptos-rpc.com"
            ).split(",") if u.strip()
        ]
    )
    aptos_node_api_key: str = field(
        default_factory=lambda: os.getenv("APTOS_NODE_API_KEY") or os.getenv("CELLANA_GRPC_API_KEY", "")
    )
    aptos_max_gas: int = field(
        default_factory=lambda: int(os.getenv("APTOS_MAX_GAS", "200000"))
    )
    # Default on-chain slippage tolerance (%) for DEX swaps
    dex_swap_slippage_pct: float = field(
        default_factory=lambda: float(os.getenv("DEX_SWAP_SLIPPAGE_PCT", "0.7"))
    )
    # DEX swap retry settings (CEX-first-then-DEX strategy)
    dex_swap_max_retries: int = field(
        default_factory=lambda: int(os.getenv("DEX_SWAP_MAX_RETRIES", "2"))
    )
    dex_swap_retry_delay_s: float = field(
        default_factory=lambda: float(os.getenv("DEX_SWAP_RETRY_DELAY_S", "1.0"))
    )
    # Minimum seconds between two trade executions (cooldown)
    trade_cooldown_s: float = field(
        default_factory=lambda: float(os.getenv("TRADE_COOLDOWN_S", "2.0"))
    )
    # Minimum APT on wallet for gas before attempting DEX swap (0=disabled)
    min_gas_apt: float = field(
        default_factory=lambda: float(os.getenv("MIN_GAS_APT", "0.05"))
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




    # ------------------------------------------------------------------ #
    #  Rebalance settings
    # ------------------------------------------------------------------ #
    rebalance_enabled: bool = field(
        default_factory=lambda: os.getenv("REBALANCE_ENABLED", "false").lower() == "true"
    )
    rebalance_interval_min: float = field(
        default_factory=lambda: float(os.getenv("REBALANCE_INTERVAL_MIN", "30.0"))
    )
    
    # Dynamic Sizing (Optimize trade size based on actual balance)
    use_dynamic_sizing: bool = field(
        default_factory=lambda: os.getenv("USE_DYNAMIC_SIZING", "true").lower() == "true"
    )
    min_dynamic_trade_size_usdt: float = field(
        default_factory=lambda: float(os.getenv("MIN_DYNAMIC_TRADE_SIZE_USDT", "5.0"))
    ) # Minimum size to still consider a trade

    stop_after_trade: bool = field(
        default_factory=lambda: os.getenv("STOP_AFTER_TRADE", "false").lower() == "true"
    )

    # Address to withdraw to (usually your Aptos wallet)
    aptos_address: str = field(
        default_factory=lambda: os.getenv("APTOS_WALLET_ADDRESS", "")
    )
    
    # Optional manual deposit address for Bybit (fallback if API returns empty)
    bybit_deposit_address: str = field(
        default_factory=lambda: os.getenv("BYBIT_DEPOSIT_ADDRESS", "")
    )
    
    # Thresholds
    min_apt_threshold: float = field(
        default_factory=lambda: float(os.getenv("MIN_APT_THRESHOLD", "40.0"))
    )
    min_ami_threshold: float = field(
        default_factory=lambda: float(os.getenv("MIN_AMI_THRESHOLD", "8000.0"))
    )
    min_usdt_threshold: float = field(
        default_factory=lambda: float(os.getenv("MIN_USDT_THRESHOLD", "80.0"))
    )

    # Withdraw Networks (default to Aptos)
    bybit_withdraw_chain: str = "APTOS"
    mexc_withdraw_network: str = "APTOS(APT)"


settings = Settings()
