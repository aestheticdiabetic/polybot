"""
config_override.py — persisted strategy overrides from dashboard live-edit.
Loaded AFTER config.py to override defaults.
Call load_overrides() in main.py after importing STRATEGY.
"""
import json
import logging
import os
from pathlib import Path

log = logging.getLogger("config_override")

OVERRIDE_FILE = Path("/app/data/config.override.env")


def load_overrides() -> None:
    """Load persisted overrides from env file and apply to STRATEGY + BOND config."""
    if not OVERRIDE_FILE.exists():
        return

    try:
        from dotenv import dotenv_values
        import config as _config
        from config import STRATEGY

        overrides = dotenv_values(OVERRIDE_FILE)
        applied = {}

        # ── ARBI strategy overrides ────────────────────────────────
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

        # ── BOND numeric overrides ─────────────────────────────────
        _bond_float = {
            "BOND_MIN_EDGE_CHEAP", "BOND_MIN_EDGE_CORE",
            "BOND_MAX_CAPITAL_PER_CLUSTER",
            "BOND_EARLY_EXIT_PRICE", "BOND_CHEAP_EXIT_MULTIPLIER", "BOND_CHEAP_MIN_ABS_GAIN",
        }
        _bond_int = {
            "BOND_GAS_FLOOR_HOURS", "BOND_SHARES_CORE", "BOND_SHARES_CHEAP_MAX",
            "BOND_POLL_INTERVAL_SECS", "BOND_MAX_MARKETS_PER_RUN",
        }
        for key in _bond_float:
            if key in overrides:
                val = float(overrides[key])
                setattr(_config, key, val)
                applied[key] = val
        for key in _bond_int:
            if key in overrides:
                val = int(overrides[key])
                setattr(_config, key, val)
                applied[key] = val

        # ── BOND set overrides (stored as JSON arrays) ─────────────
        _bond_set = {
            "BOND_DISABLED_TIERS",
            "BOND_DISABLED_SIDES",
            "BOND_DISABLED_ENTRY_BUCKETS",
        }
        for key in _bond_set:
            json_key = key + "_JSON"
            if json_key in overrides:
                try:
                    val = set(json.loads(overrides[json_key]))
                    setattr(_config, key, val)
                    applied[key] = val
                except (json.JSONDecodeError, TypeError) as e:
                    log.warning(f"Failed to load {json_key}: {e}")

        # ── BOND city / alias overrides (stored as JSON) ───────────
        if "BOND_CITIES_JSON" in overrides:
            try:
                cities = json.loads(overrides["BOND_CITIES_JSON"])
                _config.BOND_CITIES = {k: tuple(v) for k, v in cities.items()}
                applied["BOND_CITIES"] = f"({len(_config.BOND_CITIES)} cities)"
            except (json.JSONDecodeError, TypeError) as e:
                log.warning(f"Failed to load BOND_CITIES_JSON: {e}")

        if "BOND_CITY_ALIASES_JSON" in overrides:
            try:
                _config.BOND_CITY_ALIASES = json.loads(overrides["BOND_CITY_ALIASES_JSON"])
                applied["BOND_CITY_ALIASES"] = f"({len(_config.BOND_CITY_ALIASES)} aliases)"
            except (json.JSONDecodeError, TypeError) as e:
                log.warning(f"Failed to load BOND_CITY_ALIASES_JSON: {e}")

        if "BOND_CITY_BIAS_CORRECTIONS_JSON" in overrides:
            try:
                _config.BOND_CITY_BIAS_CORRECTIONS = json.loads(
                    overrides["BOND_CITY_BIAS_CORRECTIONS_JSON"]
                )
                applied["BOND_CITY_BIAS_CORRECTIONS"] = (
                    f"({len(_config.BOND_CITY_BIAS_CORRECTIONS)} cities)"
                )
            except (json.JSONDecodeError, TypeError) as e:
                log.warning(f"Failed to load BOND_CITY_BIAS_CORRECTIONS_JSON: {e}")

        if applied:
            log.info(f"Loaded config overrides: {applied}")
    except Exception as e:
        log.error(f"Failed to load config overrides: {e}")


def save_overrides(overrides: dict) -> bool:
    """Persist config overrides to env file.

    Keys that are dicts (BOND_CITIES, BOND_CITY_ALIASES) are serialised as JSON
    under *_JSON keys. All other values are stored as plain key=value.
    """
    try:
        # Read existing overrides
        existing: dict = {}
        if OVERRIDE_FILE.exists():
            from dotenv import dotenv_values
            existing = dict(dotenv_values(OVERRIDE_FILE))

        _set_keys = {"BOND_DISABLED_TIERS", "BOND_DISABLED_SIDES", "BOND_DISABLED_ENTRY_BUCKETS"}

        # Merge new overrides
        for k, v in overrides.items():
            if k == "BOND_CITIES":
                # Serialise dict[str, tuple] to JSON
                existing["BOND_CITIES_JSON"] = json.dumps(
                    {name: list(coords) for name, coords in v.items()}
                )
            elif k == "BOND_CITY_ALIASES":
                existing["BOND_CITY_ALIASES_JSON"] = json.dumps(v)
            elif k == "BOND_CITY_BIAS_CORRECTIONS":
                existing["BOND_CITY_BIAS_CORRECTIONS_JSON"] = json.dumps(v, sort_keys=True)
            elif k in _set_keys:
                existing[k + "_JSON"] = json.dumps(sorted(v))
            else:
                existing[k] = str(v)

        # Write back
        OVERRIDE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(OVERRIDE_FILE, "w") as f:
            for k, v in existing.items():
                # JSON values may contain = so we write as key=<value> with no extra quoting
                # dotenv_values handles this correctly on reload
                f.write(f"{k}={v}\n")
        log.info(f"Persisted config overrides: {list(overrides.keys())}")
        return True
    except Exception as e:
        log.error(f"Failed to persist config overrides: {e}")
        return False
