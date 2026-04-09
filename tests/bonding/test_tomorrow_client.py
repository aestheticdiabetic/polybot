"""Tests for tomorrow.io forecast client."""
import asyncio
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../bot"))

from datetime import date
from unittest.mock import AsyncMock, patch, MagicMock
import pytest


def _make_tio_response(target_date: date, temp_max: float) -> dict:
    return {
        "data": {
            "timelines": [
                {
                    "timestep": "1d",
                    "intervals": [
                        {
                            "startTime": f"{target_date.isoformat()}T06:00:00Z",
                            "values": {"temperatureMax": temp_max},
                        }
                    ],
                }
            ]
        }
    }


def test_get_forecast_returns_none_if_no_api_key(monkeypatch):
    import config as _config
    monkeypatch.setattr(_config, "TOMORROW_IO_API_KEY", "")
    from bonding import tomorrow_client
    tomorrow_client._disk_cache_loaded = False
    tomorrow_client._cache.clear()
    tomorrow_client._call_times.clear()

    result = asyncio.run(
        tomorrow_client.get_forecast("London", 51.5, -0.1, date(2026, 4, 15))
    )
    assert result is None


def test_get_forecast_returns_forecast_result(monkeypatch):
    import config as _config
    monkeypatch.setattr(_config, "TOMORROW_IO_API_KEY", "test-key")
    monkeypatch.setattr(_config, "TOMORROW_IO_CACHE_TTL_SECS", 10800)
    from bonding import tomorrow_client
    tomorrow_client._disk_cache_loaded = True   # skip disk load
    tomorrow_client._cache.clear()
    tomorrow_client._call_times.clear()

    target = date(2026, 4, 15)
    raw = _make_tio_response(target, 18.5)

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=raw)
    mock_resp.raise_for_status = MagicMock()
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        with patch.object(tomorrow_client, "_save_disk_cache"):
            result = asyncio.run(
                tomorrow_client.get_forecast("London", 51.5, -0.1, target)
            )

    assert result is not None
    assert result.city == "London"
    assert result.target_date == target
    assert result.daily_max_c == pytest.approx(18.5)
    assert len(result.ensemble_members) == 100


def test_get_forecast_returns_none_on_api_error(monkeypatch):
    import config as _config
    monkeypatch.setattr(_config, "TOMORROW_IO_API_KEY", "test-key")
    monkeypatch.setattr(_config, "TOMORROW_IO_CACHE_TTL_SECS", 10800)
    from bonding import tomorrow_client
    tomorrow_client._disk_cache_loaded = True
    tomorrow_client._cache.clear()
    tomorrow_client._call_times.clear()

    with patch("aiohttp.ClientSession", side_effect=RuntimeError("connection error")):
        result = asyncio.run(
            tomorrow_client.get_forecast("London", 51.5, -0.1, date(2026, 4, 15))
        )
    assert result is None


def test_get_forecast_returns_none_on_rate_limit(monkeypatch):
    import config as _config
    monkeypatch.setattr(_config, "TOMORROW_IO_API_KEY", "test-key")
    monkeypatch.setattr(_config, "TOMORROW_IO_CACHE_TTL_SECS", 10800)
    from bonding import tomorrow_client
    import time
    tomorrow_client._disk_cache_loaded = True
    tomorrow_client._cache.clear()
    # Fill up the rate limit log
    tomorrow_client._call_times.clear()
    tomorrow_client._call_times.extend([time.time()] * 20)

    result = asyncio.run(
        tomorrow_client.get_forecast("London", 51.5, -0.1, date(2026, 4, 15))
    )
    assert result is None


def test_cache_hit_skips_api_call(monkeypatch):
    import config as _config
    import time
    monkeypatch.setattr(_config, "TOMORROW_IO_API_KEY", "test-key")
    monkeypatch.setattr(_config, "TOMORROW_IO_CACHE_TTL_SECS", 10800)
    from bonding import tomorrow_client
    tomorrow_client._disk_cache_loaded = True
    tomorrow_client._call_times.clear()

    target = date(2026, 4, 15)
    raw = _make_tio_response(target, 20.0)
    cache_key = f"51.5|-0.1|{target.isoformat()}"
    tomorrow_client._cache[cache_key] = (time.time(), raw)

    with patch("aiohttp.ClientSession") as mock_session_cls:
        result = asyncio.run(
            tomorrow_client.get_forecast("London", 51.5, -0.1, target)
        )
        mock_session_cls.assert_not_called()

    assert result is not None
    assert result.daily_max_c == pytest.approx(20.0)
