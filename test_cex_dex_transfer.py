import asyncio
import os
from typing import Optional
from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient
from aptos_sdk.transactions import TransactionPayload, EntryFunction
from aptos_sdk.type_tag import TypeTag, StructTag
from aptos_sdk.account_address import AccountAddress

from config.settings import settings
from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from utils.logger import get_logger

logger = get_logger()

# Constants
APT_METADATA_ADDRESS = "0x1"
APTOS_COIN_TYPE = "0x1::aptos_coin::AptosCoin"

class DexTransferHelper:
    def __init__(self):
        priv_key = settings.aptos_private_key
        if not priv_key:
            raise ValueError("APTOS_PRIVATE_KEY not found")
        self.account = Account.load_key(priv_key)
        self.client = RestClient(settings.aptos_node_url)
        
    async def transfer_apt(self, to_address: str, amount: float):
        """Transfer APT from DEX wallet to CEX deposit address."""
        amount_octas = int(amount * 10**8)
        logger.info(f"📤 Transferring {amount} APT to {to_address}...")
        
        payload = TransactionPayload(
            EntryFunction.standard(
                "0x1::aptos_account",
                "transfer",
                [],
                [
                    AccountAddress.from_str(to_address),
                    amount_octas
                ]
            )
        )
        
        try:
            signed_tx = await self._submit_tx(payload)
            logger.success(f"✅ APT Transfer successful! TX: {signed_tx}")
            return signed_tx
        except Exception as e:
            logger.error(f"❌ APT Transfer failed: {e}")
            return None

    async def transfer_ami(self, to_address: str, amount: float):
        """Transfer AMI (Fungible Asset) from DEX wallet to CEX deposit address."""
        amount_octas = int(amount * 10**8)
        logger.info(f"📤 Transferring {amount} AMI to {to_address}...")
        
        # Use primary_fungible_store::transfer
        payload = TransactionPayload(
            EntryFunction.standard(
                "0x1::primary_fungible_store",
                "transfer",
                [TypeTag(StructTag.from_str("0x1::fungible_asset::Metadata"))],
                [
                    AccountAddress.from_str(settings.ami_token_address),
                    AccountAddress.from_str(to_address),
                    amount_octas
                ]
            )
        )
        
        try:
            signed_tx = await self._submit_tx(payload)
            logger.success(f"✅ AMI Transfer successful! TX: {signed_tx}")
            return signed_tx
        except Exception as e:
            logger.error(f"❌ AMI Transfer failed: {e}")
            return None

    async def _submit_tx(self, payload):
        tx = await self.client.create_bcs_transaction(self.account, payload)
        signed_tx = self.client.create_bcs_signed_transaction(self.account, tx)
        tx_hash = await self.client.submit_bcs_transaction(signed_tx)
        await self.client.wait_for_transaction(tx_hash)
        return tx_hash

async def main():
    print("🚀 [CEX <-> DEX] Transfer Test Script (Auto-Address Enabled)")
    
    # 1. Initialize
    bybit = BybitTrader()
    mexc = MexcTrader()
    dex_helper = DexTransferHelper()
    
    # --- CONFIG ---
    TEST_APT_AMOUNT = 0.5
    TEST_AMI_AMOUNT = 100
    
    # Get addresses from settings or env
    DEX_WALLET = settings.aptos_address
    
    BYBIT_DEPOSIT_ADDR = os.getenv("BYBIT_APT_DEPOSIT_ADDRESS", "")
    MEXC_DEPOSIT_ADDR = os.getenv("MEXC_AMI_DEPOSIT_ADDRESS", "")
    
    print(f"\nSettings:")
    print(f"- Dex Wallet: {DEX_WALLET}")
    
    menu = """
    Choose action:
    1. Withdraw APT (Bybit -> DEX)
    2. Withdraw AMI (MEXC -> DEX)
    3. Deposit APT (DEX -> Bybit) [Auto-fetch addr]
    4. Deposit AMI (DEX -> MEXC) [Auto-fetch addr]
    q. Quit
    """
    
    while True:
        print(menu)
        choice = input("Enter choice: ").strip().lower()
        
        if choice == '1':
            print(f"Executing: Withdraw {TEST_APT_AMOUNT} APT from Bybit...")
            # For Bybit V5, chain must often be "APT" for Aptos network
            await bybit.withdraw("APT", TEST_APT_AMOUNT, DEX_WALLET, "APT")
        
        elif choice == '2':
            print(f"Executing: Withdraw {TEST_AMI_AMOUNT} AMI from MEXC...")
            await mexc.withdraw("AMI", TEST_AMI_AMOUNT, DEX_WALLET, settings.mexc_withdraw_network)
            
        elif choice == '3':
            addr = BYBIT_DEPOSIT_ADDR
            if not addr:
                print("📡 Fetching Bybit deposit address via API...")
                addr = await bybit.get_deposit_address("APT", settings.bybit_withdraw_chain)
            
            if addr:
                print(f"Executing: Deposit {TEST_APT_AMOUNT} APT to Bybit ({addr})...")
                await dex_helper.transfer_apt(addr, TEST_APT_AMOUNT)
            else:
                print("❌ Failed to get Bybit deposit address.")
            
        elif choice == '4':
            addr = MEXC_DEPOSIT_ADDR
            if not addr:
                print("📡 Fetching MEXC deposit address via API...")
                addr = await mexc.get_deposit_address("AMI", settings.mexc_withdraw_network)
            
            if addr:
                print(f"Executing: Deposit {TEST_AMI_AMOUNT} AMI to MEXC ({addr})...")
                await dex_helper.transfer_ami(addr, TEST_AMI_AMOUNT)
            else:
                print("❌ Failed to get MEXC deposit address.")
            
        elif choice == 'q':
            break

if __name__ == "__main__":
    asyncio.run(main())
