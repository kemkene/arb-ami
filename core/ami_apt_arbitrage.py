"""
AMI/APT Arbitrage Calculator

Calculates arbitrage opportunities between:
- Cellana DEX (on-chain): AMI/APT pool
- CEX exchanges (Bybit/MEXC): AMI/USDT and APT/USDT

Price flow:
1. Get AMI/APT price from Cellana SyncEvent (reserves-based)
2. Get AMI/USDT price from CEX 
3. Get APT/USDT price from CEX
4. Calculate arbitrage opportunities
"""

from typing import Dict, Optional, Tuple
from utils.logger import get_logger

logger = get_logger()


class AmiAptArbitrageCalculator:
    """Calculate arbitrage opportunities for AMI/APT between Cellana DEX and CEX."""
    
    # Fee configurations
    CELLANA_FEE = 0.001  # 0.1% for volatile pools (default)
    CEX_FEE = 0.001      # 0.1% typical CEX fee
    
    # Minimum profit threshold (in %)
    MIN_PROFIT_PCT = 0.5
    
    def __init__(self, cex_fee: float = 0.001, dex_fee: float = 0.001):
        """Initialize calculator with fee configuration."""
        self.cex_fee = cex_fee
        self.dex_fee = dex_fee
    
    def calculate_dex_to_cex_opportunity(
        self,
        dex_price_ami_per_apt: float,
        cex_price_ami_usdt: float,
        cex_price_apt_usdt: float,
    ) -> Dict[str, any]:
        """Calculate arbitrage: Buy AMI on DEX, sell on CEX.
        
        Flow:
        1. Buy AMI with APT on Cellana
        2. Sell AMI for USDT on CEX
        3. Sell APT for USDT on CEX (to get initial capital back)
        
        Args:
            dex_price_ami_per_apt: Price of AMI in APT on Cellana (e.g., 0.008 = 1 AMI costs 0.008 APT)
            cex_price_ami_usdt: AMI/USDT price on CEX
            cex_price_apt_usdt: APT/USDT price on CEX
        
        Returns:
            Dict with opportunity details
        """
        # Calculate implied AMI/APT price on CEX
        # If 1 AMI = 10 USDT and 1 APT = 100 USDT
        # Then 1 AMI = 0.1 APT on CEX
        cex_implied_ami_per_apt = cex_price_ami_usdt / cex_price_apt_usdt
        
        # Calculate profit percentage (before fees)
        # If DEX price is 0.008 and CEX implied is 0.01
        # We can buy cheap on DEX and sell expensive on CEX
        price_diff_pct = ((cex_implied_ami_per_apt - dex_price_ami_per_apt) / dex_price_ami_per_apt) * 100
        
        # Calculate net profit after fees
        # DEX fee: buying AMI
        # CEX fee: selling AMI
        total_fees_pct = (self.dex_fee + self.cex_fee) * 100
        net_profit_pct = price_diff_pct - total_fees_pct
        
        is_profitable = net_profit_pct >= self.MIN_PROFIT_PCT
        
        return {
            "direction": "DEX_TO_CEX",
            "dex_price": dex_price_ami_per_apt,
            "cex_implied_price": cex_implied_ami_per_apt,
            "price_diff_pct": price_diff_pct,
            "fees_pct": total_fees_pct,
            "net_profit_pct": net_profit_pct,
            "is_profitable": is_profitable,
            "details": {
                "cex_ami_usdt": cex_price_ami_usdt,
                "cex_apt_usdt": cex_price_apt_usdt,
            }
        }
    
    def calculate_cex_to_dex_opportunity(
        self,
        dex_price_ami_per_apt: float,
        cex_price_ami_usdt: float,
        cex_price_apt_usdt: float,
    ) -> Dict[str, any]:
        """Calculate arbitrage: Buy AMI on CEX, sell on DEX.
        
        Flow:
        1. Buy AMI with USDT on CEX
        2. Sell AMI for APT on Cellana
        3. Sell APT for USDT on CEX (to get initial capital back)
        
        Args:
            dex_price_ami_per_apt: Price of AMI in APT on Cellana
            cex_price_ami_usdt: AMI/USDT price on CEX
            cex_price_apt_usdt: APT/USDT price on CEX
        
        Returns:
            Dict with opportunity details
        """
        # Calculate implied AMI/APT price on CEX
        cex_implied_ami_per_apt = cex_price_ami_usdt / cex_price_apt_usdt
        
        # Calculate profit percentage (before fees)
        # If DEX price is 0.01 and CEX implied is 0.008
        # We can buy cheap on CEX and sell expensive on DEX
        price_diff_pct = ((dex_price_ami_per_apt - cex_implied_ami_per_apt) / cex_implied_ami_per_apt) * 100
        
        # Calculate net profit after fees
        # CEX fee: buying AMI and selling APT
        # DEX fee: selling AMI for APT
        total_fees_pct = (self.cex_fee * 2 + self.dex_fee) * 100  # 2 CEX trades + 1 DEX trade
        net_profit_pct = price_diff_pct - total_fees_pct
        
        is_profitable = net_profit_pct >= self.MIN_PROFIT_PCT
        
        return {
            "direction": "CEX_TO_DEX",
            "dex_price": dex_price_ami_per_apt,
            "cex_implied_price": cex_implied_ami_per_apt,
            "price_diff_pct": price_diff_pct,
            "fees_pct": total_fees_pct,
            "net_profit_pct": net_profit_pct,
            "is_profitable": is_profitable,
            "details": {
                "cex_ami_usdt": cex_price_ami_usdt,
                "cex_apt_usdt": cex_price_apt_usdt,
            }
        }
    
    def find_best_opportunity(
        self,
        dex_price_ami_per_apt: float,
        cex_price_ami_usdt: float,
        cex_price_apt_usdt: float,
    ) -> Optional[Dict[str, any]]:
        """Find the best arbitrage opportunity between DEX and CEX.
        
        Returns the most profitable direction, or None if no profitable opportunity.
        """
        dex_to_cex = self.calculate_dex_to_cex_opportunity(
            dex_price_ami_per_apt, cex_price_ami_usdt, cex_price_apt_usdt
        )
        cex_to_dex = self.calculate_cex_to_dex_opportunity(
            dex_price_ami_per_apt, cex_price_ami_usdt, cex_price_apt_usdt
        )
        
        # Return the most profitable opportunity
        if dex_to_cex["is_profitable"] or cex_to_dex["is_profitable"]:
            if dex_to_cex["net_profit_pct"] >= cex_to_dex["net_profit_pct"]:
                return dex_to_cex
            else:
                return cex_to_dex
        
        return None
    
    def log_opportunity(self, opportunity: Dict[str, any]) -> None:
        """Log arbitrage opportunity details."""
        if not opportunity:
            return
        
        direction = opportunity["direction"]
        profit = opportunity["net_profit_pct"]
        
        if direction == "DEX_TO_CEX":
            logger.info(
                f"🎯 ARBITRAGE OPPORTUNITY: Buy AMI on Cellana, Sell on CEX | "
                f"Net Profit: {profit:.2f}% | "
                f"DEX Price: {opportunity['dex_price']:.8f} | "
                f"CEX Implied: {opportunity['cex_implied_price']:.8f}"
            )
        else:
            logger.info(
                f"🎯 ARBITRAGE OPPORTUNITY: Buy AMI on CEX, Sell on Cellana | "
                f"Net Profit: {profit:.2f}% | "
                f"DEX Price: {opportunity['dex_price']:.8f} | "
                f"CEX Implied: {opportunity['cex_implied_price']:.8f}"
            )
