"""Country economic-reference service.

Provides per-country economic anchors (median income proxy + poverty line)
so the rule engine can calibrate income thresholds to the deployment country
instead of a single hardcoded scale. Data comes from the World Bank API,
cached in-memory for 24h (one call per country per day, never per
beneficiary). Any failure degrades to a neutral fallback profile — this
module NEVER raises and NEVER blocks a scan.

Poverty-line methodology (per user decision):
    Anchored on the World Bank lower-middle-income line of $3.65/day
    (2017 PPP), expressed monthly and scaled per country by the ratio of
    that country's GNI per capita to a lower-middle-income reference GNI.
    Richer countries get a proportionally higher poverty line (relative
    poverty), poorer countries a lower one — so the same beneficiary income
    reads as "poverty" in one country but not another.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

# ── World Bank config ────────────────────────────────────────────────────────
_WB_BASE = "https://api.worldbank.org/v2"
_GNI_INDICATOR = "NY.GNP.PCAP.CD"  # GNI per capita, Atlas method (current US$)
_HTTP_TIMEOUT = 8  # seconds — short so a slow WB never stalls a scan

# ── Poverty-line anchors (documented, per user decision) ─────────────────────
_POVERTY_LINE_DAILY_USD = 3.65   # World Bank lower-middle-income line (2017 PPP)
_DAYS_PER_MONTH = 30.4
# Monthly value of the $3.65/day line for a country sitting exactly at the
# lower-middle-income reference GNI. ≈ $111/month per person.
_POVERTY_ANCHOR_MONTHLY = round(_POVERTY_LINE_DAILY_USD * _DAYS_PER_MONTH, 2)
# GNI/capita of the reference lower-middle-income country at which $3.65/day
# IS the national poverty line. Used only as the scaling denominator.
_GNI_REFERENCE_USD = 2800.0

# ── Neutral fallback (used on any network/data failure) ──────────────────────
# poverty_line ≈ 100 reproduces the engine's original implicit calibration
# (the old absolute rules were income_per_person < 50 / < 70), so an offline
# scan behaves close to the pre-country-aware system rather than mis-flagging.
_FALLBACK_PROFILE = {
    "gni_per_capita_usd": _GNI_REFERENCE_USD,
    "median_income": round(_GNI_REFERENCE_USD / 12, 2),  # monthly per-person proxy
    "poverty_line": 100.0,
    "data_source": "fallback",
    "use_fallback": True,
}

# ── In-memory cache: {country_code: (profile_dict, fetched_at_epoch)} ─────────
_CACHE_TTL_SECONDS = 24 * 3600
_FALLBACK_RETRY_SECONDS = 60  # retry soon after a transient failure, not next day
_cache: dict[str, tuple[dict, float]] = {}
_lock = threading.Lock()


def _monthly_poverty_line(gni_per_capita_usd: float) -> float:
    """Scale the $3.65/day monthly anchor by the country's GNI ratio."""
    ratio = gni_per_capita_usd / _GNI_REFERENCE_USD if _GNI_REFERENCE_USD else 1.0
    return round(_POVERTY_ANCHOR_MONTHLY * ratio, 2)


def _fetch_gni_from_worldbank(country_code: str) -> float | None:
    """Return the most-recent non-null GNI/capita for a country, or None.

    Uses an explicit recent date range rather than the `mrnev=1` shortcut:
    mrnev returns HTTP 400 for some country codes (confirmed for Senegal),
    while a date range reliably returns rows ordered most-recent-first.
    """
    url = (
        f"{_WB_BASE}/country/{country_code}/indicator/{_GNI_INDICATOR}"
        f"?format=json&per_page=10&date=2015:2025"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "fraud-engine/1.0"})
    with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    # WB shape: [metadata, [ {country:{value}, value: <gni>, date:...}, ... ]]
    if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
        return None
    for row in payload[1]:
        val = row.get("value")
        if val is not None:
            return float(val)
    return None


def _build_profile(country_code: str) -> dict:
    """Fetch fresh data and build a profile; fall back neutrally on any error."""
    code = country_code.upper()
    try:
        gni = _fetch_gni_from_worldbank(code)
        if gni is None or gni <= 0:
            logger.warning("No GNI data for %s — using fallback profile", code)
            return {"country_code": code, **_FALLBACK_PROFILE}
        profile = {
            "country_code": code,
            "gni_per_capita_usd": round(gni, 2),
            "median_income": round(gni / 12, 2),  # monthly per-person proxy
            "poverty_line": _monthly_poverty_line(gni),
            "data_source": "worldbank",
            "use_fallback": False,
        }
        return profile
    except (urllib.error.URLError, TimeoutError, ValueError, OSError) as exc:
        logger.warning("World Bank fetch failed for %s (%s) — using fallback",
                       code, exc)
        return {"country_code": code, **_FALLBACK_PROFILE}
    except Exception as exc:  # never let this module block a scan
        logger.warning("Unexpected country-profile error for %s (%s) — fallback",
                       code, exc)
        return {"country_code": code, **_FALLBACK_PROFILE}


def get_country_profile(country_code: str) -> dict:
    """Return the economic profile for a country, cached for 24h.

    Always returns a dict with at least:
        country_code, median_income, poverty_line, data_source, use_fallback
    Never raises. On any failure returns a neutral fallback (use_fallback=True).
    """
    code = (country_code or "").upper().strip()
    if not code:
        return {"country_code": "", **_FALLBACK_PROFILE}

    now = time.time()
    with _lock:
        cached = _cache.get(code)
        if cached:
            profile, fetched_at = cached
            # Fallback results (a transient network blip, WB down, etc.) are
            # cached only briefly so the NEXT request retries soon instead of
            # being stuck on a bad blip for a full day. Only genuine live
            # World Bank data gets the full 24h TTL.
            ttl = _CACHE_TTL_SECONDS if not profile.get("use_fallback") else _FALLBACK_RETRY_SECONDS
            if (now - fetched_at) < ttl:
                return dict(profile)

    # Fetch outside the lock (network call); a concurrent duplicate fetch for
    # the same new country is harmless and rare (once per country per day).
    profile = _build_profile(code)

    with _lock:
        _cache[code] = (profile, now)
    return dict(profile)


def clear_cache() -> None:
    """Test/ops helper: drop all cached profiles."""
    with _lock:
        _cache.clear()
