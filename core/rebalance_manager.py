"""
RebalanceManager: Handles automatic transfers between CEX and DEX to ensure liquidity.
"""
import asyncio
import time
from typing import Optional

from config.settings import settings
from utils.logger import get_logger
from utils.telegram_notifier import notifier as tg_notifier
from core.balance_manager import BalanceManager
from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from exchanges.aptos_trader import AptosTrader

logger = get_logger()

class RebalanceManager:
    def __init__(
        self,
        balance_manager: BalanceManager,
        bybit_trader: BybitTrader,
        mexc_trader: MexcTrader,
    ):
        self.balance_manager = balance_manager
        self.bybit_trader = bybit_trader
        self.mexc_trader = mexc_trader
        self.aptos_trader = AptosTrader()
        self._is_running = False
        self._last_check_ts = 0.0
        self.interval_sec = settings.rebalance_interval_min * 60

        # Define 'Max' thresholds for DEX (move to settings if needed)
        # If we have 3x the minimum required, it's excess.
        self.max_apt_dex = settings.min_apt_threshold * 3
        self.max_ami_dex = settings.min_ami_threshold * 3

    async def start(self):
        """Start the background rebalance loop."""
        if not settings.rebalance_enabled:
            logger.info("⚖️ RebalanceManager is disabled in settings.")
            return

        self._is_running = True
        logger.info(f"⚖️ RebalanceManager started (Interval: {settings.rebalance_interval_min} min)")
        
        while self._is_running:
            try:
                await self.check_and_rebalance()
            except Exception as e:
                logger.error(f"❌ RebalanceManager error: {e}")
            
            await asyncio.sleep(60) # Check every minute if it's time to run

    async def stop(self):
        self._is_running = False
        await self.aptos_trader.close()

    async def check_and_rebalance(self, force: bool = False):
        now = time.time()
        time_since_last = now - self._last_check_ts
        
        if not force and self._last_check_ts > 0 and time_since_last < self.interval_sec:
            mins_left = (self.interval_sec - time_since_last) / 60
            logger.debug(f"⚖️ [REBALANCE] Skipping check (Next check in {mins_left:.1f} min)")
            return

        self._last_check_ts = now
        tag = " [FORCED]" if force else ""
        logger.info(f"⚖️ [REBALANCE]{tag} Checking balances...")
        
        # Ensure balances are fresh
        await self.balance_manager.refresh()
        
        # 1. Handle DEX Depletion (Top Up from CEX)
        await self._check_dex_shortage()

        # 2. Handle CEX Depletion (Top Up from DEX) - NEW Logic
        await self._check_cex_shortage()

        # 2.5 Ensure Bybit funds are in Trading Account
        await self._sync_bybit_funding_to_trading()

        # 3. Handle DEX Overflow (Excess to CEX)
        await self._check_dex_excess()

        # 4. Check USDT on CEX (Bybit + MEXC total)
        usdt_bybit = self.balance_manager.get_free("bybit", "USDT")
        usdt_mexc = self.balance_manager.get_free("mexc", "USDT")
        total_usdt = usdt_bybit + usdt_mexc
        
        if total_usdt < settings.min_usdt_threshold:
            msg = f"⚠️ [REBALANCE] Total USDT on CEX is CRITICAL: ${total_usdt:.2f} (Threshold: ${settings.min_usdt_threshold})"
            logger.error(msg)
            # await tg_notifier.send_message(msg)

    async def _check_dex_shortage(self):
        """Top up DEX if low."""
        apt_dex = self.balance_manager.get_free("dex", "APT")
        if apt_dex < settings.min_apt_threshold:
            logger.warning(f"⚖️ [REBALANCE] APT on DEX is LOW ({apt_dex:.2f} < {settings.min_apt_threshold}). Topping up.")
            await self._top_up_apt(settings.min_apt_threshold - apt_dex + 5.0)
        else:
            logger.debug(f"⚖️ [REBALANCE] DEX APT OK ({apt_dex:.2f} >= {settings.min_apt_threshold})")

        ami_dex = self.balance_manager.get_free("dex", "AMI")
        if ami_dex < settings.min_ami_threshold:
            logger.warning(f"⚖️ [REBALANCE] AMI on DEX is LOW ({ami_dex:.0f} < {settings.min_ami_threshold}). Topping up.")
            await self._top_up_ami(settings.min_ami_threshold - ami_dex + 500)
        else:
            logger.debug(f"⚖️ [REBALANCE] DEX AMI OK ({ami_dex:.0f} >= {settings.min_ami_threshold})")

    async def _sync_bybit_funding_to_trading(self):
        """Move any funds from Bybit Funding account to Unified Trading account."""
        try:
            funding_balances = await self.bybit_trader.get_funding_balances()
            for coin, amount in funding_balances.items():
                if amount > 0:
                    logger.info(f"🔄 [REBALANCE] Detected {amount} {coin} in Bybit FUNDING. Moving to UNIFIED...")
                    success = await self.bybit_trader.internal_transfer(coin, amount, "FUND", "UNIFIED")
                    if success:
                        # Wait a bit for Bybit to update internal records
                        await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"❌ RebalanceManager: Bybit account sync failed: {e}")

    async def _check_cex_shortage(self):
        """Top up CEX from DEX if DEX has a safety buffer."""
        apt_dex = self.balance_manager.get_free("dex", "APT")
        ami_dex = self.balance_manager.get_free("dex", "AMI")

        # Buffers: Only send if DEX stays well above MIN
        apt_buffer = settings.min_apt_threshold + 5.0
        ami_buffer = settings.min_ami_threshold + 2000

        # Check Bybit
        apt_bybit = self.balance_manager.get_free("bybit", "APT")
        if apt_bybit < settings.min_apt_threshold:
            if apt_dex > apt_buffer:
                needed = settings.min_apt_threshold - apt_bybit + 5.0
                logger.info(f"⚖️ [REBALANCE] Bybit APT is LOW ({apt_bybit:.2f} < {settings.min_apt_threshold}). Sending {needed:.2f} from DEX.")
                await self._deposit_to_specific_cex("bybit", "APT", needed)
            else:
                logger.warning(f"⚖️ [REBALANCE] Bybit APT is LOW ({apt_bybit:.2f}) but DEX buffer is insufficient ({apt_dex:.2f} <= {apt_buffer}). Skipping.")
        else:
            logger.debug(f"⚖️ [REBALANCE] Bybit APT OK ({apt_bybit:.2f} >= {settings.min_apt_threshold})")

        ami_bybit = self.balance_manager.get_free("bybit", "AMI")
        if ami_bybit < settings.min_ami_threshold:
            if ami_dex > ami_buffer:
                needed = settings.min_ami_threshold - ami_bybit + 500
                logger.info(f"⚖️ [REBALANCE] Bybit AMI is LOW ({ami_bybit:.0f} < {settings.min_ami_threshold}). Sending {needed:.0f} from DEX.")
                await self._deposit_to_specific_cex("bybit", "AMI", needed)
            else:
                logger.warning(f"⚖️ [REBALANCE] Bybit AMI is LOW ({ami_bybit:.0f}) but DEX buffer is insufficient ({ami_dex:.0f} <= {ami_buffer}). Skipping.")

        # Check MEXC
        apt_mexc = self.balance_manager.get_free("mexc", "APT")
        if apt_mexc < settings.min_apt_threshold:
            if apt_dex > apt_buffer:
                needed = settings.min_apt_threshold - apt_mexc + 5.0
                logger.info(f"⚖️ [REBALANCE] MEXC APT is LOW ({apt_mexc:.2f} < {settings.min_apt_threshold}). Sending {needed:.2f} from DEX.")
                await self._deposit_to_specific_cex("mexc", "APT", needed)
            else:
                logger.warning(f"⚖️ [REBALANCE] MEXC APT is LOW ({apt_mexc:.2f}) but DEX buffer is insufficient ({apt_dex:.2f} <= {apt_buffer}). Skipping.")
        else:
            logger.debug(f"⚖️ [REBALANCE] MEXC APT OK ({apt_mexc:.2f} >= {settings.min_apt_threshold})")

        ami_mexc = self.balance_manager.get_free("mexc", "AMI")
        if ami_mexc < settings.min_ami_threshold:
            if ami_dex > ami_buffer:
                needed = settings.min_ami_threshold - ami_mexc + 500
                logger.info(f"⚖️ [REBALANCE] MEXC AMI is LOW ({ami_mexc:.0f} < {settings.min_ami_threshold}). Sending {needed:.0f} from DEX.")
                await self._deposit_to_specific_cex("mexc", "AMI", needed)
            else:
                logger.warning(f"⚖️ [REBALANCE] MEXC AMI is LOW ({ami_mexc:.0f}) but DEX buffer is insufficient ({ami_dex:.0f} <= {ami_buffer}). Skipping.")

    async def _check_dex_excess(self):
        """Move extreme excess DEX funds back to CEX (Security Overflow)."""
        apt_dex = self.balance_manager.get_free("dex", "APT")
        if apt_dex > self.max_apt_dex:
            excess = apt_dex - (settings.min_apt_threshold * 1.5)
            logger.info(f"⚖️ [REBALANCE] APT on DEX is EXCESS ({apt_dex:.2f} > {self.max_apt_dex}). Moving {excess:.2f} to CEX.")
            await self._deposit_to_cex("APT", excess)

        ami_dex = self.balance_manager.get_free("dex", "AMI")
        if ami_dex > self.max_ami_dex:
            excess = ami_dex - (settings.min_ami_threshold * 1.5)
            logger.info(f"⚖️ [REBALANCE] AMI on DEX is EXCESS ({ami_dex:.0f} > {self.max_ami_dex}). Moving {excess:.0f} to CEX.")
            await self._deposit_to_cex("AMI", excess)

    async def _deposit_to_specific_cex(self, exchange: str, coin: str, amount: float):
        """Deposit to a specific CEX by name."""
        addr = None
        if exchange == "bybit":
            addr = await self.bybit_trader.get_deposit_address(coin, settings.bybit_withdraw_chain)
        elif exchange == "mexc":
            addr = await self.mexc_trader.get_deposit_address(coin, settings.mexc_withdraw_network)
        
        if not addr:
            logger.error(f"❌ [REBALANCE] Could not get deposit address for {coin} on {exchange}")
            return

        if coin == "APT":
            await self.aptos_trader.deposit_apt(addr, amount)
        elif coin == "AMI":
            await self.aptos_trader.deposit_ami(addr, amount)

    async def _deposit_to_cex(self, coin: str, amount: float):
        """Find best CEX to deposit (MEXC preferred for ease of use) and execute."""
        # This is for General Excess overflow
        target = "mexc"
        await self._deposit_to_specific_cex(target, coin, amount)

    async def _top_up_apt(self, amount_needed: float):
        """Find where APT is available and withdraw to DEX."""
        bybit_apt = self.balance_manager.get_free("bybit", "APT")
        if bybit_apt >= amount_needed + 1.0:
            logger.info(f"⚖️ [REBALANCE] Withdrawing {amount_needed:.2f} APT from Bybit to {settings.aptos_address}")
            await self.bybit_trader.withdraw(
                coin="APT",
                amount=amount_needed,
                address=settings.aptos_address,
                chain=settings.bybit_withdraw_chain
            )
            return

        mexc_apt = self.balance_manager.get_free("mexc", "APT")
        if mexc_apt >= amount_needed + 1.0:
            logger.info(f"⚖️ [REBALANCE] Withdrawing {amount_needed:.2f} APT from MEXC to {settings.aptos_address}")
            await self.mexc_trader.withdraw(
                coin="APT",
                amount=amount_needed,
                address=settings.aptos_address,
                network=settings.mexc_withdraw_network
            )
            return
        
        logger.error(f"❌ [REBALANCE] Cannot top up APT. Insufficient APT on both Bybit ({bybit_apt:.2f}) and MEXC ({mexc_apt:.2f})")

    async def _top_up_ami(self, amount_needed: float):
        """Find where AMI is available and withdraw to DEX."""
        mexc_ami = self.balance_manager.get_free("mexc", "AMI")
        if mexc_ami >= amount_needed + 100:
            logger.info(f"⚖️ [REBALANCE] Withdrawing {amount_needed:.0f} AMI from MEXC to {settings.aptos_address}")
            await self.mexc_trader.withdraw(
                coin="AMI",
                amount=amount_needed,
                address=settings.aptos_address,
                network=settings.mexc_withdraw_network
            )
            return

        bybit_ami = self.balance_manager.get_free("bybit", "AMI")
        if bybit_ami >= amount_needed + 100:
            logger.info(f"⚖️ [REBALANCE] Withdrawing {amount_needed:.0f} AMI from Bybit to {settings.aptos_address}")
            await self.bybit_trader.withdraw(
                coin="AMI",
                amount=amount_needed,
                address=settings.aptos_address,
                chain=settings.bybit_withdraw_chain
            )
            return

        logger.error(f"❌ [REBALANCE] Cannot top up AMI. Insufficient AMI on both Bybit ({bybit_ami:.0f}) and MEXC ({mexc_ami:.0f})")
