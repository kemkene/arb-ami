"""
Backward-compatible wrapper around the unified PanoraClient.

Prefer using PanoraClient from exchanges.panora directly.
"""

from exchanges.panora import PanoraClient


class PanoraPrice(PanoraClient):
    """Convenience subclass that pre-configures token addresses."""

    async def get_price(self, from_token_amount: float):
        return await self.get_swap_quote(from_token_amount)

