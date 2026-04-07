"""
market_scanner.py — Poll Polymarket Gamma API for open weather/temperature markets.

Read-only. Parses natural language market questions to extract structured data
(city, date, temperature bucket). No orders are placed here.
"""
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional

import aiohttp

from bonding.weather_client import _resolve_city, UnknownCityError

log = logging.getLogger("bond.scanner")

GAMMA_API  = "https://gamma-api.polymarket.com"
CLOB_API   = "https://clob.polymarket.com"

# Gamma API returns up to 500 markets per page; we page if needed
GAMMA_PAGE_LIMIT = 500

# Month abbreviation → number
_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


@dataclass
class MarketCandidate:
    market_id: str
    token_id: str              # YES outcome token ID (used for CLOB orders)
    question: str
    city: str                  # canonical city name
    target_date: date
    temp_min: Optional[float]  # lower bound of temperature bucket (°C)
    temp_max: Optional[float]  # upper bound of temperature bucket (°C)
    unit: str                  # "C" or "F"
    best_ask: float            # current best ask for YES outcome
    resolution_time: datetime
    ask_book: list = field(default_factory=list)  # [(price, size), ...] ascending; empty = depth unknown


async def scan_weather_markets() -> list[MarketCandidate]:
    """
    Query Gamma API for open weather/temperature markets.
    Parse each question, resolve city, fetch orderbook ask.
    Returns list of fully-populated MarketCandidate objects.
    """
    raw_markets = await _fetch_gamma_markets()
    log.info(f"BOND_GAMMA_FETCH total_raw={len(raw_markets)}")

    candidates: list[MarketCandidate] = []
    fail_parse  = 0
    fail_city   = 0
    fail_token  = 0
    fail_ask    = 0
    fail_time   = 0
    unknown_cities: dict[str, int] = {}

    for m in raw_markets:
        question = m.get("question", "")
        parsed = parse_market_question(question)
        if parsed is None:
            fail_parse += 1
            continue

        # Resolve city to canonical name
        try:
            canonical, _, _ = _resolve_city(parsed["city"])
        except UnknownCityError:
            fail_city += 1
            city = parsed["city"]
            unknown_cities[city] = unknown_cities.get(city, 0) + 1
            continue

        # Get YES token ID + embedded price from Gamma response
        token_id, gamma_price = _extract_yes_token_and_price(m)
        if not token_id:
            fail_token += 1
            continue

        # Use Gamma-embedded price if available; fall back to CLOB orderbook.
        # When using Gamma price we have no depth info — ask_book stays empty.
        if gamma_price is not None and 0.0 < gamma_price < 1.0:
            best_ask = gamma_price
            ask_book: list = []
        else:
            ask_book = await _get_ask_book(token_id)
            if not ask_book:
                fail_ask += 1
                continue
            best_ask = ask_book[0][0]

        if best_ask <= 0.0 or best_ask >= 1.0:
            fail_ask += 1
            continue

        # Parse resolution time
        resolution_time = _parse_resolution_time(m)
        if resolution_time is None:
            fail_time += 1
            continue

        candidates.append(MarketCandidate(
            market_id=m.get("id", ""),
            token_id=token_id,
            question=question,
            city=canonical,
            target_date=parsed["date"],
            temp_min=parsed.get("temp_min"),
            temp_max=parsed.get("temp_max"),
            unit=parsed.get("unit", "C"),
            best_ask=best_ask,
            resolution_time=resolution_time,
            ask_book=ask_book,
        ))

    log.info(
        f"BOND_SCAN_COMPLETE total={len(raw_markets)} "
        f"fail_parse={fail_parse} fail_city={fail_city} "
        f"fail_token={fail_token} fail_ask={fail_ask} fail_time={fail_time} "
        f"qualifying={len(candidates)}"
    )
    if unknown_cities:
        top = sorted(unknown_cities, key=lambda c: -unknown_cities[c])[:15]
        log.info(f"BOND_UNKNOWN_CITIES (not in city list, top 15 by frequency): {top}")

    return candidates


def extract_unknown_cities(raw_markets: list[dict]) -> dict[str, int]:
    """
    Scan raw Gamma market dicts for cities that appear in parsed questions
    but are not in BOND_CITIES. Returns {city_name: occurrence_count}.
    """
    unknown: dict[str, int] = {}
    for m in raw_markets:
        question = m.get("question", "")
        parsed = parse_market_question(question)
        if not parsed:
            continue
        city = parsed.get("city", "")
        if not city:
            continue
        try:
            _resolve_city(city)
        except UnknownCityError:
            unknown[city] = unknown.get(city, 0) + 1
    return unknown


def parse_market_question(question: str) -> Optional[dict]:
    """
    Extract city, date, temp_min, temp_max, unit from a Polymarket question string.

    Handled patterns:
    - "Highest temperature in Tokyo on April 7?"
    - "Will the highest temperature in Munich be 22°C on April 8?"
    - "Will the highest temperature in Buenos Aires be 19°C on April 8?"
    - "Daily high in London above 18°C on April 9?"
    - "Highest temperature in Los Angeles on April 7?" (single bucket = exact °C)
    - "17°C" / "80°F" / "17-18°C" / "80-81°F" — temperature forms

    Returns dict with keys: city, date, temp_min, temp_max, unit
    Returns None if parsing fails.
    """
    q = question.strip()

    # ── Extract city ──────────────────────────────────────────────
    city = _extract_city(q)
    if not city:
        return None

    # ── Extract date ──────────────────────────────────────────────
    target_date = _extract_date(q)
    if target_date is None:
        return None

    # ── Extract temperature bucket ────────────────────────────────
    unit, temp_min, temp_max = _extract_temp_bucket(q)

    return {
        "city":     city,
        "date":     target_date,
        "temp_min": temp_min,
        "temp_max": temp_max,
        "unit":     unit,
    }


async def _fetch_gamma_markets() -> list[dict]:
    """
    GET /markets from Gamma API, filtering for weather/temperature tags.
    Pages through results if needed.
    """
    all_markets: list[dict] = []
    offset = 0
    timeout = aiohttp.ClientTimeout(total=15)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            params = {
                "active":       "true",
                "closed":       "false",
                "limit":        GAMMA_PAGE_LIMIT,
                "offset":       offset,
                "tag_slug":     "weather",  # primary filter
            }
            try:
                async with session.get(f"{GAMMA_API}/markets", params=params) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            except Exception as exc:
                log.warning(f"scanner: Gamma API fetch failed: {exc}")
                break

            # Gamma returns a list or a dict with "data" key depending on version
            if isinstance(data, list):
                page = data
            else:
                page = data.get("data", data.get("markets", []))

            if not page:
                break

            # Also accept temperature-related markets not tagged weather
            all_markets.extend(page)

            if len(page) < GAMMA_PAGE_LIMIT:
                break
            offset += GAMMA_PAGE_LIMIT

    # Deduplicate by id
    seen: set[str] = set()
    unique: list[dict] = []
    for m in all_markets:
        mid = m.get("id", "")
        if mid and mid not in seen:
            seen.add(mid)
            unique.append(m)

    # Client-side keyword filter: only keep temperature-related questions.
    # Guards against tag_slug being ignored by the API (which returns all ~50k markets).
    _TEMP_RE = re.compile(
        r"temperature|°[CcFf]|\bF\b|\bC\b|degrees?|daily.?high|highest.?temp|high.?temp",
        re.IGNORECASE,
    )
    filtered = [m for m in unique if _TEMP_RE.search(m.get("question", ""))]
    log.info(f"BOND_GAMMA_KEYWORD_FILTER raw={len(unique)} after_filter={len(filtered)}")
    return filtered


def _extract_city(question: str) -> Optional[str]:
    """
    Extract city name from question text.
    Tries several patterns in order of specificity.
    """
    patterns = [
        # "temperature in <City> on"
        r"temperature in ([A-Z][A-Za-z\s]+?) on\b",
        # "temperature in <City> be"
        r"temperature in ([A-Z][A-Za-z\s]+?) (?:be|above|below)\b",
        # "high in <City> on"
        r"high in ([A-Z][A-Za-z\s]+?) on\b",
        # "high in <City> above/below/be"
        r"high in ([A-Z][A-Za-z\s]+?) (?:above|below|be)\b",
        # "in <City> on <Month>"
        r"in ([A-Z][A-Za-z\s]+?) on (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)",
        # "in <City>?" at end
        r"in ([A-Z][A-Za-z\s]{2,30})\?",
    ]
    for pat in patterns:
        m = re.search(pat, question)
        if m:
            city = m.group(1).strip().rstrip("?.,")
            # Clean trailing common words
            city = re.sub(r"\s+(on|be|above|below|the|a)\s*$", "", city, flags=re.IGNORECASE).strip()
            if len(city) >= 2:
                return city
    return None


def _extract_date(question: str) -> Optional[date]:
    """
    Extract target date from question.
    Handles: "April 7", "Apr 7", "April 7th", "7 April".
    Falls back to current year; increments year if date is in the past.
    """
    # "April 7" / "April 7th" / "Apr 7"
    patterns = [
        r"(?:on\s+)?([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s*[?,]|$|\?)",
        r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)",
    ]
    for pat in patterns:
        m = re.search(pat, question, re.IGNORECASE)
        if not m:
            continue
        g1, g2 = m.group(1).lower(), m.group(2).lower()
        # Determine which is month and which is day
        if g1 in _MONTH_MAP:
            month, day = _MONTH_MAP[g1], int(g2)
        elif g2 in _MONTH_MAP:
            month, day = _MONTH_MAP[g2], int(g1)
        else:
            continue
        year = datetime.now(timezone.utc).year
        try:
            d = date(year, month, day)
        except ValueError:
            continue
        # If date already passed this year, assume next year
        if d < date.today():
            try:
                d = date(year + 1, month, day)
            except ValueError:
                continue
        return d
    return None


def _extract_temp_bucket(question: str) -> tuple[str, Optional[float], Optional[float]]:
    """
    Extract temperature bucket (unit, temp_min, temp_max) from question.

    Returns (unit, temp_min, temp_max):
    - unit: "C" or "F"
    - temp_min/max: None if not found (scorer will fall back to prob_in_range
      over all buckets or treat as exact match)

    Handled forms:
    - "22°C"          → C, 21.5, 22.5 (±0.5 around exact)
    - "17-18°C"       → C, 17, 18
    - "80°F"          → F, 79.5, 80.5
    - "80-81°F"       → F, 80, 81
    - "above 18°C"    → C, 18, 35  (open upper bound capped at 35°C)
    - "below 10°C"    → C, -20, 10
    - "be 22°C"       → same as exact
    """
    # Range: "17-18°C" or "80-81°F"
    m = re.search(r"(\d+(?:\.\d+)?)[–\-](\d+(?:\.\d+)?)\s*°?\s*([CF])\b", question, re.IGNORECASE)
    if m:
        lo, hi, unit = float(m.group(1)), float(m.group(2)), m.group(3).upper()
        return unit, lo, hi

    # Above threshold: "above 18°C"
    m = re.search(r"above\s+(\d+(?:\.\d+)?)\s*°?\s*([CF])\b", question, re.IGNORECASE)
    if m:
        val, unit = float(m.group(1)), m.group(2).upper()
        upper = 45.0 if unit == "C" else 120.0
        return unit, val, upper

    # Below threshold: "below 10°C"
    m = re.search(r"below\s+(\d+(?:\.\d+)?)\s*°?\s*([CF])\b", question, re.IGNORECASE)
    if m:
        val, unit = float(m.group(1)), m.group(2).upper()
        lower = -30.0 if unit == "C" else -20.0
        return unit, lower, val

    # Exact: "be 22°C" / "22°C"
    m = re.search(r"(?:be\s+)?(\d+(?:\.\d+)?)\s*°?\s*([CF])\b", question, re.IGNORECASE)
    if m:
        val, unit = float(m.group(1)), m.group(2).upper()
        return unit, val - 0.5, val + 0.5

    return "C", None, None


def _extract_yes_token_and_price(market: dict) -> tuple[Optional[str], Optional[float]]:
    """
    Extract the token_id AND current price for the YES outcome from a Gamma market dict.
    Gamma embeds prices so we can avoid a separate CLOB orderbook call in most cases.

    Returns (token_id, price) — price is None if not found in Gamma response.
    """
    token_id: Optional[str] = None
    price: Optional[float]  = None

    # Shape 1: tokens list [{"outcome": "Yes", "token_id": "...", "price": "0.65"}, ...]
    tokens = market.get("tokens", [])
    for tok in tokens:
        if str(tok.get("outcome", "")).lower() in ("yes", "1"):
            token_id = tok.get("token_id") or tok.get("tokenId")
            raw = tok.get("price")
            if raw is not None:
                try:
                    price = float(raw)
                except (ValueError, TypeError):
                    pass
            break

    # Shape 2: clob_token_ids list (index 0 = YES by convention)
    if not token_id:
        clob_ids = market.get("clobTokenIds") or market.get("clob_token_ids", [])
        if clob_ids:
            token_id = clob_ids[0]

    # Price fallback: outcomePrices parallel to outcomes (index 0 = YES)
    if price is None:
        outcome_prices = market.get("outcomePrices", [])
        if outcome_prices:
            try:
                price = float(outcome_prices[0])
            except (ValueError, TypeError):
                pass

    # Price fallback 2: top-level price/lastPrice fields
    if price is None:
        for key in ("price", "lastPrice", "lastTradePrice"):
            raw = market.get(key)
            if raw is not None:
                try:
                    price = float(raw)
                    break
                except (ValueError, TypeError):
                    pass

    return token_id, price


async def _get_ask_book(token_id: str) -> list[tuple[float, float]]:
    """
    Fetch the full ask side of the CLOB order book for a token.
    Returns a list of (price, size) tuples sorted ascending by price.
    Returns [] on failure or empty book.
    """
    timeout = aiohttp.ClientTimeout(total=5)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                f"{CLOB_API}/book", params={"token_id": token_id}
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                asks = data.get("asks", [])
                if not asks:
                    return []
                book: list[tuple[float, float]] = []
                for a in asks:
                    try:
                        book.append((float(a["price"]), float(a.get("size", 0))))
                    except (KeyError, ValueError):
                        continue
                book.sort(key=lambda x: x[0])
                return book
    except Exception as exc:
        log.debug(f"scanner: orderbook fetch failed for {token_id[:12]}...: {exc}")
    return []


def _parse_resolution_time(market: dict) -> Optional[datetime]:
    """Parse resolution/end time from Gamma market dict."""
    for key in ("end_date_iso", "endDateIso", "endDate", "end_date", "resolutionTime"):
        val = market.get(key)
        if val:
            try:
                dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
                return dt.astimezone(timezone.utc)
            except ValueError:
                continue
    return None
