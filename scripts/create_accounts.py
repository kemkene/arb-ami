"""
create_accounts.py — Generate Aptos wallets and save to aptos_wallets.csv.

Usage:
    python scripts/create_accounts.py --count 3
    python scripts/create_accounts.py --count 1 --output my_aptos.csv
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from aptos_sdk.account import Account as AptosAccount

load_dotenv(Path(__file__).parent.parent / ".env")

OUTPUT_DEFAULT = "aptos_wallets.csv"


@dataclass
class AptosRecord:
    index: int
    address: str = ""
    private_key: str = ""
    public_key: str = ""
    created_at: str = field(
        default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds") + "Z"
    )


def generate_wallet() -> tuple[str, str, str]:
    """Return (address, private_key_hex, public_key_hex)."""
    acc = AptosAccount.generate()
    return str(acc.address()), acc.private_key.hex(), str(acc.public_key())


def run(count: int, output: str) -> None:
    output_path = Path(__file__).parent.parent / output

    records: list[AptosRecord] = []
    for i in range(1, count + 1):
        addr, priv, pub = generate_wallet()
        rec = AptosRecord(index=i, address=addr, private_key=priv, public_key=pub)
        records.append(rec)
        print(f"  [{i}/{count}] {addr}")
        print(f"           private_key: {priv}")

    fieldnames = list(asdict(records[0]).keys())
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(asdict(rec))

    print(f"\n✅ Saved {len(records)} wallet(s) → {output_path}")
    print("⚠️  Keep this file safe — it contains private keys!\n")


def main() -> None:
    p = argparse.ArgumentParser(description="Generate Aptos wallets.")
    p.add_argument("--count",  type=int, default=1,
                   help="Number of wallets to generate (default: 1)")
    p.add_argument("--output", type=str, default=OUTPUT_DEFAULT,
                   help=f"Output CSV filename (default: {OUTPUT_DEFAULT})")
    args = p.parse_args()

    print(f"\nGenerating {args.count} Aptos wallet(s)...\n")
    run(args.count, args.output)


if __name__ == "__main__":
    main()
