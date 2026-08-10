"""
Wallet helpers — solders keypair from a base58 private key.
"""

from __future__ import annotations

from solders.keypair import Keypair


def get_keypair(private_key_b58: str) -> Keypair:
    """Build a solders Keypair from a base58 private key string."""
    if not private_key_b58:
        raise ValueError("PRIVATE_KEY is empty — set it in .env")
    return Keypair.from_base58_string(private_key_b58)


def pubkey_str(keypair: Keypair) -> str:
    """Public key (wallet address) as a string."""
    return str(keypair.pubkey())
