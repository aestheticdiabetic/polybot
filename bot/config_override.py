"""
config_override.py — persisted strategy overrides from dashboard live-edit.
Loaded AFTER config.py to override defaults.
Call load_overrides() in main.py after importing STRATEGY.
"""
import os
import logging
from pathlib import Path

log = logging.getLogger("config_override")

OVERRIDE_FILE = Path("/app/config.override.env")


def load_overrides() -> None:
    """Load persisted overrides from .env file and apply to STRATEGY."""
    if not OVERRIDE_FILE.exists():
        return

    try:
        from dotenv import dotenv_values
        from config import STRATEGY

        overrides = dotenv_values(OVERRIDE_FILE)
        applied = {}

        # Type-cast and apply each override
        if "bracket_threshold" in overrides:
            val = float(overrides["bracket_threshold"])
            STRATEGY.bracket_threshold = val
            applied["bracket_threshold"] = val

        if "position_size_usdc" in overrides:
            val = float(overrides["position_size_usdc"])
            STRATEGY.position_size_usdc = val
            applied["position_size_usdc"] = val

        if "max_concurrent_brackets" in overrides:
            val = int(overrides["max_concurrent_brackets"])
            STRATEGY.max_concurrent_brackets = val
            applied["max_concurrent_brackets"] = val

        if "cancel_unfilled_after_s" in overrides:
            val = int(overrides["cancel_unfilled_after_s"])
            STRATEGY.cancel_unfilled_after_s = val
            applied["cancel_unfilled_after_s"] = val

        if applied:
            log.info(f"Loaded config overrides: {applied}")
    except Exception as e:
        log.error(f"Failed to load config overrides: {e}")


def save_overrides(overrides: dict) -> bool:
    """Persist config overrides to .env file."""
    try:
        # Read existing overrides
        existing = {}
        if OVERRIDE_FILE.exists():
            from dotenv import dotenv_values
            existing = dict(dotenv_values(OVERRIDE_FILE))

        # Merge new overrides
        existing.update(overrides)

        # Write back
        OVERRIDE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(OVERRIDE_FILE, "w") as f:
            for k, v in existing.items():
                f.write(f"{k}={v}\n")
        log.info(f"Persisted config overrides: {overrides}")
        return True
    except Exception as e:
        log.error(f"Failed to persist config overrides: {e}")
        return False
