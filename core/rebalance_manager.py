"""
RebalanceManager — monitor balances and perform automated withdrawals
from CEX (Bybit/MEXC) to DEX (Aptos) to maintain minimum operational thresholds.
"""
import asyncio
import time
from typing import Dict, Optional, TYPE_CHECKING
from config.settings import settings
from utils.logger import get_logger

if TYPE_CHECKING:
    from core.balance_manager import BalanceManager
    from utils.telegram_notifier import TelegramNotifier

logger = get_logger()

class RebalanceManager:
    """
    Coordinates asset movement between exchanges.
    Current Focus: CEX -> DEX (Aptos) for APT and AMI.
    """

    def __init__(
        self,
        balance_manager: "BalanceManager",
        telegram: Optional["TelegramNotifier"] = None,
        check_interval_s: float = 1800.0,  # Default 30 minutes
    ) -> None:
        self.bm = balance_manager
        self.telegram = telegram
        self.check_interval_s = check_interval_s
        
        # User-defined thresholds
        self.thresholds = {
            "USDT": 80.0,
            "APT": 40.0,
            "AMI": 8000.0
        }
        
        # Withdrawal configurations (Chain/Network names for CEX)
        self.configs = {
            "bybit": {
                "APT": {"chain": "Aptos"},
                "AMI": {"chain": "Aptos"}
            },
            "mexc": {
                "APT": {"network": "Aptos"},
                "AMI": {"network": "Aptos"}
            }
        }
        
        self._is_running = False
        self._last_check_ts = 0.0

    async def run_loop(self) -> None:
        """Background task for periodic rebalance checks."""
        logger.info(f"⚖️ RebalanceManager: Starting loop (interval={self.check_interval_s/60:.1f}m)")
        self._is_running = True
        
        # Initial wait to let bot stabilize
        await asyncio.sleep(60)
        
        while self._is_running:
            try:
                await self.check_and_rebalance()
            except Exception as e:
                logger.error(f"RebalanceManager loop error: {e}")
            
            await asyncio.sleep(self.check_interval_s)

    async def check_and_rebalance(self) -> None:
        """Main logic to detect shortages and trigger withdrawals."""
        logger.info("⚖️ RebalanceManager: Checking balances for rebalance...")
        await self.bm.refresh()
        
        # 1. Check APT on DEX
        dex_apt = self.bm.get_free("dex", "APT")
        if dex_apt < self.thresholds["APT"]:
            needed = self.thresholds["APT"] * 1.5 - dex_apt # Refill to 150% of threshold
            logger.warning(f"⚖️ [REBALANCE] APT on DEX low ({dex_apt:.2f} < {self.thresholds['APT']}). Need ~{needed:.2f}")
            await self._refill_from_cex("APT", needed)
            
        # 2. Check AMI on DEX
        dex_ami = self.bm.get_free("dex", "AMI")
        if dex_ami < self.thresholds["AMI"]:
            needed = self.thresholds["AMI"] * 1.5 - dex_ami
            logger.warning(f"⚖️ [REBALANCE] AMI on DEX low ({dex_ami:.1f} < {self.thresholds['AMI']}). Need ~{needed:.0f}")
            await self._refill_from_cex("AMI", needed)
            
        # 3. Check USDT on CEX (Notification only)
        for exch in ["bybit", "mexc"]:
            usdt = self.bm.get_free(exch, "USDT")
            if usdt < self.thresholds["USDT"]:
                msg = f"⚠️ [REBALANCE ALERT] {exch.upper()} USDT is low: {usdt:.2f} < {self.thresholds['USDT']}. Please deposit manually!"
                logger.warning(msg)
                if self.telegram:
                    await self.telegram.send_message(msg)

        self._last_check_ts = time.time()

    async def _refill_from_cex(self, asset: str, amount_needed: float) -> bool:
        """Attempt to withdraw from a CEX that has enough surplus."""
        target_address = settings.aptos_address
        if not target_address:
            logger.error("RebalanceManager: No Aptos address configured for withdrawal!")
            return False

        # Try Bybit first, then MEXC
        exchanges = []
        if self.bm.bybit_trader: exchanges.append(("bybit", self.bm.bybit_trader))
        if self.bm.mexc_trader: exchanges.append(("mexc", self.bm.mexc_trader))
        
        for exch_name, trader in exchanges:
            free = self.bm.get_free(exch_name, asset)
            # Only withdraw if we have enough surplus on CEX (keep at least threshold on CEX too)
            if free > amount_needed + self.thresholds.get(asset, 0):
                logger.info(f"⚖️ [REBALANCE] Withdrawing {amount_needed} {asset} from {exch_name.upper()} back to DEX...")
                
                if self.telegram:
                    await self.telegram.send_message(f"⚖️ <b>Rebalance:</b> Withdrawing {amount_needed:.2f} {asset} from {exch_name.upper()} to ví Aptos...")

                res = None
                if exch_name == "bybit":
                    res = await trader.withdraw(
                        coin=asset,
                        amount=amount_needed,
                        address=target_address,
                        chain=self.configs["bybit"][asset]["chain"]
                    )
                elif exch_name == "mexc":
                    res = await trader.withdraw(
                        coin=asset,
                        address=target_address,
                        amount=amount_needed,
                        network=self.configs["mexc"][asset]["network"]
                    )
                
                if res:
                    logger.success(f"⚖️ [REBALANCE] Withdrawal submitted! ID: {res}")
                    return True
        
        logger.warning(f"⚖️ [REBALANCE] Could not find CEX with enough surplus {asset} for refill.")
        return False

    def stop(self) -> None:
        self._is_running = False
