"""
Central configuration for Bicycle RTI (Real-Time Intelligence) Hackathon.
=========================================================================
Reads TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME from .env so every script
uses the same credentials without hardcoding them.

Usage:
    from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME
"""

import os
import sys
from pathlib import Path


def _load_env():
    """Load key=value pairs from .env file into os.environ."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        print("=" * 60)
        print("  ERROR: .env file not found!")
        print("=" * 60)
        print(f"  Expected at: {env_path}")
        print()
        print("  Quick fix:")
        print("    copy .env.template .env")
        print("    Then edit .env with YOUR tenant and admin email.")
        print("=" * 60)
        sys.exit(1)

    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


_load_env()

TENANT_ID = os.environ["TENANT_ID"]
ADMIN_ACCOUNT = os.environ["ADMIN_ACCOUNT"]
WORKSPACE_NAME = os.environ.get("WORKSPACE_NAME", "Bike Rental Hackathon")
