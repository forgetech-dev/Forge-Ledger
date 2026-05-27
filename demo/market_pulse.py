import csv
import io
import json
import os
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEMO_DIR = Path(__file__).resolve().parent
CONFIG_FILE = DEMO_DIR / "config.json"
MARKET_PULSE_CACHE_FILE = DEMO_DIR / "market_pulse_cache.json"
USER_AGENT = "Forge-Ledger-MarketPulse/1.0"
CBOE_DAILY_PRICES_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/{symbol}_History.csv"
FRANKFURTER_TIMESERIES_URL = "https://api.frankfurter.app/{start}..{end}"

DEFAULT_MARKET_PULSE_SYMBOLS = [
    {
        "key": "sp500",
        "symbol": "SPY",
        "name": "S&P 500",
        "subtitle": "SPY proxy",
        "group": "index",
    },
    {
        "key": "nasdaq100",
        "symbol": "QQQ",
        "name": "Nasdaq 100",
        "subtitle": "QQQ proxy",
        "group": "index",
    },
    {
        "key": "vix",
        "symbol": "VIX",
        "name": "VIX",
        "subtitle": "Cboe VIX Index",
        "group": "volatility",
        "source": "cboe_history",
    },
    {
        "key": "vxn",
        "symbol": "VXN",
        "name": "VXN",
        "subtitle": "Cboe Nasdaq-100 Volatility Index",
        "group": "volatility",
        "source": "cboe_history",
    },
    {
        "key": "gold",
        "symbol": "GLD",
        "name": "Gold",
        "subtitle": "GLD proxy",
        "group": "macro",
    },
    {
        "key": "treasury",
        "symbol": "IEF",
        "name": "Treasury Bonds",
        "subtitle": "7-10Y Treasury ETF proxy",
        "group": "macro",
    },
    {
        "key": "usd_cny",
        "symbol": "USD/CNY",
        "name": "USD to CNY",
        "subtitle": "Exchange rate",
        "group": "fx",
        "source": "frankfurter_fx",
        "quote_currency": "CNY",
    },
    {
        "key": "usd_jpy",
        "symbol": "USD/JPY",
        "name": "USD to JPY",
        "subtitle": "Exchange rate",
        "group": "fx",
        "source": "frankfurter_fx",
        "quote_currency": "JPY",
    },
]

CACHE_LOCK = threading.Lock()
REFRESH_THREAD_STARTED = False
REFRESH_JOB_LOCK = threading.Lock()
REFRESH_JOB_RUNNING = False


def _read_json(path, default):
    if not path.exists():
        return default
    try:
        text = path.read_text()
        return json.loads(text) if text.strip() else default
    except (OSError, json.JSONDecodeError):
        return default


def _write_json(path, data):
    path.write_text(json.dumps(data, indent=2))


def _json_clone(data):
    return json.loads(json.dumps(data))


def _int_setting(data, key, default, minimum):
    try:
        return max(minimum, int(data.get(key, default)))
    except (TypeError, ValueError):
        return default


def _float_setting(data, key, default, minimum):
    try:
        return max(minimum, float(data.get(key, default)))
    except (TypeError, ValueError):
        return default


def _market_symbols(data):
    raw_symbols = data.get("market_pulse_symbols")
    if not isinstance(raw_symbols, list):
        return DEFAULT_MARKET_PULSE_SYMBOLS

    symbols = []
    for index, item in enumerate(raw_symbols):
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        symbols.append(
            {
                "key": str(item.get("key") or symbol.lower() or f"symbol-{index}"),
                "symbol": symbol,
                "name": str(item.get("name") or symbol),
                "subtitle": str(item.get("subtitle") or ""),
                "group": str(item.get("group") or "macro"),
                "source": str(item.get("source") or "twelvedata"),
                "quote_currency": str(item.get("quote_currency") or ""),
            }
        )
    return symbols or DEFAULT_MARKET_PULSE_SYMBOLS


def _string_list_setting(data, key, default):
    value = data.get(key, default)
    if isinstance(value, str):
        value = [item.strip() for item in value.split(",")]
    if not isinstance(value, list):
        value = default
    return [str(item).strip().upper() for item in value if str(item).strip()]


def load_market_pulse_config():
    data = _read_json(CONFIG_FILE, {})
    if not isinstance(data, dict):
        data = {}
    interval = str(data.get("market_pulse_interval") or os.getenv("MARKET_PULSE_INTERVAL", "1day")).strip()
    if interval not in {"1day", "1week", "1month"}:
        interval = "1day"
    sparkline_interval = str(
        data.get("market_pulse_sparkline_interval")
        or os.getenv("MARKET_PULSE_SPARKLINE_INTERVAL", "5min")
    ).strip()
    if sparkline_interval not in {"1min", "5min", "15min", "30min", "45min", "1h"}:
        sparkline_interval = "5min"
    return {
        "cache_seconds": _int_setting(
            data,
            "market_pulse_cache_seconds",
            int(os.getenv("MARKET_PULSE_CACHE_SECONDS", "7200")),
            300,
        ),
        "output_size": _int_setting(
            data,
            "market_pulse_output_size",
            int(os.getenv("MARKET_PULSE_OUTPUT_SIZE", "45")),
            20,
        ),
        "interval": interval,
        "sparkline_interval": sparkline_interval,
        "sparkline_output_size": _int_setting(
            data,
            "market_pulse_sparkline_output_size",
            int(os.getenv("MARKET_PULSE_SPARKLINE_OUTPUT_SIZE", "100")),
            12,
        ),
        "intraday_symbols": set(
            _string_list_setting(
                data,
                "market_pulse_intraday_symbols",
                ["SPY", "QQQ"],
            )
        ),
        "request_delay_seconds": _float_setting(
            data,
            "market_pulse_request_delay_seconds",
            float(os.getenv("MARKET_PULSE_REQUEST_DELAY_SECONDS", "15")),
            0,
        ),
        "timeout_seconds": _float_setting(
            data,
            "market_pulse_timeout_seconds",
            float(os.getenv("MARKET_PULSE_TIMEOUT_SECONDS", "12")),
            1,
        ),
        "startup_delay_seconds": _float_setting(
            data,
            "market_pulse_startup_delay_seconds",
            float(os.getenv("MARKET_PULSE_STARTUP_DELAY_SECONDS", "180")),
            0,
        ),
        "symbols": _market_symbols(data),
    }


def empty_market_pulse_cache():
    return {"fetched_at": 0, "payload": None}


def load_market_pulse_cache():
    data = _read_json(MARKET_PULSE_CACHE_FILE, empty_market_pulse_cache())
    if not isinstance(data, dict):
        return empty_market_pulse_cache()
    data.setdefault("fetched_at", 0)
    data.setdefault("payload", None)
    return data


def save_market_pulse_cache(data):
    with CACHE_LOCK:
        _write_json(MARKET_PULSE_CACHE_FILE, data)


def reset_market_pulse_cache():
    save_market_pulse_cache(empty_market_pulse_cache())


def previous_market_pulse_instruments():
    payload = load_market_pulse_cache().get("payload")
    if not isinstance(payload, dict):
        return {}
    instruments = payload.get("instruments")
    if not isinstance(instruments, list):
        return {}

    previous = {}
    for item in instruments:
        if not isinstance(item, dict):
            continue
        for key in (item.get("key"), item.get("symbol")):
            if key:
                previous[str(key).upper()] = item
    return previous


def twelve_data_api_key():
    return os.getenv("TWELVE_DATA_API_KEY", "").strip()


def fetch_time_series(symbol, config, interval=None, output_size=None):
    interval = interval or config["interval"]
    output_size = output_size or config["output_size"]
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": str(output_size),
        "order": "asc",
        "apikey": twelve_data_api_key(),
    }
    url = "https://api.twelvedata.com/time_series?" + urlencode(params)
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=config["timeout_seconds"]) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Twelve Data HTTP {error.code}: {body[:180]}") from error
    except (URLError, TimeoutError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Twelve Data request failed: {error}") from error

    if payload.get("status") == "error":
        raise RuntimeError(payload.get("message") or payload.get("code") or "Twelve Data error")

    values = payload.get("values")
    if not isinstance(values, list):
        raise RuntimeError("Twelve Data response did not include time-series values")

    prices = []
    for point in values:
        if not isinstance(point, dict):
            continue
        date = str(point.get("datetime") or "").strip()
        try:
            close = float(point.get("close"))
        except (TypeError, ValueError):
            continue
        if date:
            prices.append({"date": date, "close": close})

    prices.sort(key=lambda point: point["date"])
    if len(prices) < 2:
        raise RuntimeError("Twelve Data returned fewer than two close prices")
    return prices


def fetch_cboe_history_prices(symbol, config):
    clean_symbol = str(symbol or "").strip().upper()
    url = CBOE_DAILY_PRICES_URL.format(symbol=clean_symbol)
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=config["timeout_seconds"]) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Cboe {clean_symbol} HTTP {error.code}: {body[:180]}") from error
    except (URLError, TimeoutError) as error:
        raise RuntimeError(f"Cboe {clean_symbol} request failed: {error}") from error

    prices = []
    for row in csv.DictReader(io.StringIO(raw)):
        try:
            month, day, year = str(row.get("DATE") or "").split("/")
            close = float(row.get("CLOSE"))
        except (TypeError, ValueError):
            continue
        prices.append({"date": f"{year}-{month.zfill(2)}-{day.zfill(2)}", "close": close})

    prices.sort(key=lambda point: point["date"])
    if len(prices) < 2:
        raise RuntimeError(f"Cboe {clean_symbol} response returned fewer than two close prices")
    return prices[-config["output_size"] :]


def fetch_frankfurter_fx_prices(symbol, config):
    clean_symbol = str(symbol or "").strip().upper()
    if "/" in clean_symbol:
        base, quote = clean_symbol.split("/", 1)
    else:
        base, quote = "USD", clean_symbol
    base = base.strip() or "USD"
    quote = quote.strip()
    if not quote:
        raise RuntimeError("FX quote currency is missing")

    # Frankfurter follows ECB business-day calendars, so request extra calendar
    # days to get enough market points after weekends and holidays.
    end = date.today()
    start = end - timedelta(days=max(config["output_size"] * 3, 45))
    params = urlencode({"from": base, "to": quote})
    url = FRANKFURTER_TIMESERIES_URL.format(
        start=start.isoformat(),
        end=end.isoformat(),
    ) + "?" + params
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=config["timeout_seconds"]) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Frankfurter {clean_symbol} HTTP {error.code}: {body[:180]}") from error
    except (URLError, TimeoutError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Frankfurter {clean_symbol} request failed: {error}") from error

    rates = payload.get("rates")
    if not isinstance(rates, dict):
        raise RuntimeError("Frankfurter response did not include rates")

    prices = []
    for day, values in rates.items():
        if not isinstance(values, dict):
            continue
        try:
            close = float(values.get(quote))
        except (TypeError, ValueError):
            continue
        prices.append({"date": str(day), "close": close})

    prices.sort(key=lambda point: point["date"])
    if len(prices) < 2:
        raise RuntimeError(f"Frankfurter {clean_symbol} returned fewer than two rates")
    return prices[-config["output_size"] :]


def pct_change(current, previous):
    if previous in (None, 0):
        return None
    return ((current - previous) / previous) * 100


def simple_rsi(prices, period=14):
    closes = [float(point["close"]) for point in prices if point.get("close") is not None]
    if len(closes) <= period:
        return None
    changes = [closes[index] - closes[index - 1] for index in range(1, len(closes))]
    window = changes[-period:]
    gains = [max(change, 0) for change in window]
    losses = [abs(min(change, 0)) for change in window]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def moving_average(prices, period=20):
    closes = [float(point["close"]) for point in prices if point.get("close") is not None]
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def classify_rsi(value):
    if value is None:
        return {"label": "Unavailable", "tone": "neutral"}
    if value < 30:
        return {"label": "Oversold", "tone": "positive"}
    if value < 50:
        return {"label": "Soft", "tone": "cautious"}
    if value <= 70:
        return {"label": "Healthy", "tone": "positive"}
    if value <= 80:
        return {"label": "Hot", "tone": "cautious"}
    return {"label": "Overbought", "tone": "negative"}


def classify_trend(latest, average):
    if average is None:
        return {"label": "Trend unavailable", "tone": "neutral"}
    if latest >= average * 1.02:
        return {"label": "Above 20D trend", "tone": "positive"}
    if latest <= average * 0.98:
        return {"label": "Below 20D trend", "tone": "negative"}
    return {"label": "Near 20D trend", "tone": "neutral"}


def instrument_from_prices(definition, prices, sparkline=None, sparkline_interval=None):
    latest = prices[-1]
    previous = prices[-2]
    latest_close = float(latest["close"])
    previous_close = float(previous["close"])
    change = latest_close - previous_close
    change_percent = pct_change(latest_close, previous_close)
    rsi = simple_rsi(prices)
    average_20d = moving_average(prices)
    return {
        **definition,
        "latest": latest_close,
        "previous": previous_close,
        "change": change,
        "change_percent": change_percent,
        "date": latest["date"],
        "prices": prices,
        "sparkline": sparkline or prices,
        "sparkline_interval": sparkline_interval or definition.get("sparkline_interval") or "1day",
        "rsi": rsi,
        "rsi_label": classify_rsi(rsi),
        "trend": classify_trend(latest_close, average_20d),
        "moving_average_20d": average_20d,
    }


def latest_intraday_session(points):
    if not points:
        return points
    dated = [point for point in points if " " in str(point.get("date") or "")]
    if not dated:
        return points
    latest_date = max(str(point["date"]).split(" ", 1)[0] for point in dated)
    session = [point for point in dated if str(point["date"]).startswith(latest_date)]
    return session if len(session) >= 2 else points


def stale_instrument_from_cache(definition, previous, error):
    cached = (
        previous.get(str(definition.get("key") or "").upper())
        or previous.get(str(definition.get("symbol") or "").upper())
    )
    if isinstance(cached, dict) and cached.get("latest") is not None:
        clone = _json_clone(cached)
        clone["stale"] = True
        clone["error"] = str(error)
        return clone

    return {
        **definition,
        "latest": None,
        "previous": None,
        "change": None,
        "change_percent": None,
        "date": "Unavailable",
        "prices": [],
        "sparkline": [],
        "sparkline_interval": None,
        "rsi": None,
        "rsi_label": {"label": "Unavailable", "tone": "neutral"},
        "trend": {"label": "Trend unavailable", "tone": "neutral"},
        "moving_average_20d": None,
        "unavailable": True,
        "error": str(error),
    }


def wait_for_rate_limit(last_request_at, config):
    wait = config["request_delay_seconds"] - (time.time() - last_request_at)
    if last_request_at and wait > 0:
        time.sleep(wait)
    return time.time()


def market_score(instruments):
    by_key = {item["key"]: item for item in instruments}
    score = 50
    notes = []

    for key, label in (("sp500", "S&P 500"), ("nasdaq100", "Nasdaq 100")):
        item = by_key.get(key)
        if not item or item.get("latest") is None:
            continue
        change = float(item.get("change_percent") or 0)
        rsi = item.get("rsi")
        trend_tone = (item.get("trend") or {}).get("tone")
        score += max(-8, min(8, change * 2))
        if trend_tone == "positive":
            score += 7
        elif trend_tone == "negative":
            score -= 7
        if rsi is not None:
            if 50 <= rsi <= 70:
                score += 5
            elif rsi > 78:
                score -= 7
            elif rsi < 35:
                score -= 3
        notes.append(f"{label} {change:+.2f}%")

    vix = by_key.get("vix")
    if vix and vix.get("latest") is not None:
        symbol = str(vix.get("symbol") or "").upper()
        if symbol == "VIX":
            level = float(vix.get("latest") or 0)
            if level < 18:
                score += 8
            elif level > 25:
                score -= 10
            notes.append(f"VIX {level:.2f}")
        else:
            change = float(vix.get("change_percent") or 0)
            if change < -1:
                score += 5
            elif change > 1:
                score -= 5
            notes.append(f"{symbol} {change:+.2f}%")

    score = round(max(0, min(100, score)))
    if score >= 75:
        label = "Risk-on"
        tone = "positive"
        summary = "Momentum is strong, but avoid chasing stretched moves."
    elif score >= 58:
        label = "Constructive"
        tone = "positive"
        summary = "Trend and momentum are broadly supportive."
    elif score >= 42:
        label = "Neutral"
        tone = "neutral"
        summary = "Signals are mixed. Keep position sizing balanced."
    elif score >= 25:
        label = "Cautious"
        tone = "cautious"
        summary = "Market tone is defensive. Wait for cleaner confirmation."
    else:
        label = "Risk-off"
        tone = "negative"
        summary = "Stress signals are elevated. Protect downside first."

    return {
        "score": score,
        "label": label,
        "tone": tone,
        "summary": summary,
        "notes": notes,
    }


def strategy_from_market(mood, instruments):
    by_key = {item["key"]: item for item in instruments}
    strategy = []

    sp500 = by_key.get("sp500")
    qqq = by_key.get("nasdaq100")
    if sp500 and qqq:
        available = sum(1 for item in (sp500, qqq) if item and item.get("latest") is not None)
        positives = sum(
            1
            for item in (sp500, qqq)
            if item.get("latest") is not None and (item.get("trend") or {}).get("tone") == "positive"
        )
        if available < 2:
            strategy.append({"label": "Trend", "value": "Partially unavailable", "tone": "cautious"})
        elif positives == 2:
            strategy.append({"label": "Trend", "value": "Uptrend intact", "tone": "positive"})
        elif positives == 1:
            strategy.append({"label": "Trend", "value": "Mixed leadership", "tone": "neutral"})
        else:
            strategy.append({"label": "Trend", "value": "Below trend", "tone": "negative"})

    if sp500 and sp500.get("rsi") is not None:
        rsi = sp500["rsi"]
        if rsi > 75:
            strategy.append({"label": "Momentum", "value": "Hot, be patient", "tone": "cautious"})
        elif rsi >= 50:
            strategy.append({"label": "Momentum", "value": "Healthy", "tone": "positive"})
        else:
            strategy.append({"label": "Momentum", "value": "Soft", "tone": "cautious"})

    vix = by_key.get("vix")
    if vix and vix.get("latest") is not None:
        symbol = str(vix.get("symbol") or "").upper()
        change = float(vix.get("change_percent") or 0)
        if symbol == "VIX":
            level = float(vix.get("latest") or 0)
            if level < 18:
                strategy.append({"label": "Volatility", "value": "Calm", "tone": "positive"})
            elif level <= 25:
                strategy.append({"label": "Volatility", "value": "Elevated", "tone": "cautious"})
            else:
                strategy.append({"label": "Volatility", "value": "Stress high", "tone": "negative"})
        elif change < -1:
            strategy.append({"label": "Volatility", "value": "Cooling", "tone": "positive"})
        elif change <= 1:
            strategy.append({"label": "Volatility", "value": "Stable", "tone": "neutral"})
        else:
            strategy.append({"label": "Volatility", "value": "Rising", "tone": "cautious"})

    if not strategy:
        strategy.append({"label": "Plan", "value": mood["summary"], "tone": mood["tone"]})
    return strategy


def collect_market_pulse():
    config = load_market_pulse_config()
    errors = []
    warnings = []
    instruments = []
    last_request_at = 0.0
    previous = previous_market_pulse_instruments()

    if not twelve_data_api_key():
        return {
            "configured": False,
            "fetched_at": int(time.time()),
            "from_cache": False,
            "stale": False,
            "instruments": [],
            "mood": {
                "score": None,
                "label": "Not configured",
                "tone": "neutral",
                "summary": "Add TWELVE_DATA_API_KEY to enable market analysis.",
                "notes": [],
            },
            "strategy": [],
            "errors": [{"source": "market_pulse", "error": "TWELVE_DATA_API_KEY is not set."}],
            "warnings": [],
        }

    for definition in config["symbols"]:
        try:
            if definition.get("source") in {"cboe_history", "cboe_vix"}:
                last_request_at = wait_for_rate_limit(last_request_at, config)
                prices = fetch_cboe_history_prices(definition["symbol"], config)
                instruments.append(
                    instrument_from_prices(
                        definition,
                        prices,
                        sparkline=prices,
                        sparkline_interval="1day",
                    )
                )
                continue

            if definition.get("source") == "frankfurter_fx":
                prices = fetch_frankfurter_fx_prices(definition["symbol"], config)
                instruments.append(
                    instrument_from_prices(
                        definition,
                        prices,
                        sparkline=prices,
                        sparkline_interval="1day",
                    )
                )
                continue

            last_request_at = wait_for_rate_limit(last_request_at, config)
            prices = fetch_time_series(
                definition["symbol"],
                config,
                interval=config["interval"],
                output_size=config["output_size"],
            )
            sparkline = prices
            sparkline_interval = config["interval"]
            if definition["symbol"].upper() in config["intraday_symbols"]:
                try:
                    last_request_at = wait_for_rate_limit(last_request_at, config)
                    sparkline = fetch_time_series(
                        definition["symbol"],
                        config,
                        interval=config["sparkline_interval"],
                        output_size=config["sparkline_output_size"],
                    )
                    sparkline = latest_intraday_session(sparkline)
                    sparkline_interval = config["sparkline_interval"]
                except Exception as spark_error:
                    warnings.append(
                        {
                            "source": "twelvedata",
                            "symbol": definition["symbol"],
                            "name": definition["name"],
                            "error": f"Intraday sparkline unavailable: {spark_error}",
                        }
                    )
                    print(
                        f"[MarketPulse] {definition['symbol']} intraday sparkline failed: {spark_error}",
                        flush=True,
                    )
            instruments.append(
                instrument_from_prices(
                    definition,
                    prices,
                    sparkline=sparkline,
                    sparkline_interval=sparkline_interval,
                )
            )
        except Exception as error:
            fallback = stale_instrument_from_cache(definition, previous, error)
            instruments.append(fallback)
            errors.append(
                {
                    "source": definition.get("source") or "twelvedata",
                    "symbol": definition["symbol"],
                    "name": definition["name"],
                    "error": str(error),
                    "using_stale": bool(fallback.get("stale")),
                }
            )
            print(f"[MarketPulse] {definition['symbol']} failed: {error}", flush=True)

    mood = market_score(instruments)
    return {
        "configured": True,
        "fetched_at": int(time.time()),
        "from_cache": False,
        "stale": False,
        "interval": config["interval"],
        "instruments": instruments,
        "mood": mood,
        "strategy": strategy_from_market(mood, instruments),
        "errors": errors,
        "warnings": warnings,
    }


def refresh_market_pulse_cache():
    payload = collect_market_pulse()
    save_market_pulse_cache({"fetched_at": int(time.time()), "payload": payload})
    print(
        f"[MarketPulse] refreshed at {payload['fetched_at']} "
        f"with {len(payload.get('instruments') or [])} instruments",
        flush=True,
    )
    return payload


def _market_pulse_refresh_job():
    global REFRESH_JOB_RUNNING
    try:
        refresh_market_pulse_cache()
    except Exception as error:
        print(f"[MarketPulse] refresh failed: {error}", flush=True)
    finally:
        with REFRESH_JOB_LOCK:
            REFRESH_JOB_RUNNING = False


def schedule_market_pulse_refresh():
    global REFRESH_JOB_RUNNING
    with REFRESH_JOB_LOCK:
        if REFRESH_JOB_RUNNING:
            return False
        REFRESH_JOB_RUNNING = True
    worker = threading.Thread(
        target=_market_pulse_refresh_job,
        name="market-pulse-refresh-on-demand",
        daemon=True,
    )
    worker.start()
    return True


def warming_market_pulse_payload(now):
    return {
        "configured": bool(twelve_data_api_key()),
        "fetched_at": now,
        "from_cache": False,
        "stale": False,
        "warming_up": True,
        "instruments": [],
        "mood": {
            "score": None,
            "label": "Warming up",
            "tone": "neutral",
            "summary": "Market Pulse is fetching fresh data in the background.",
            "notes": [],
        },
        "strategy": [],
        "errors": [],
        "warnings": [],
    }


def get_market_pulse_payload(force_refresh=False):
    config = load_market_pulse_config()
    cache = load_market_pulse_cache()
    payload = cache.get("payload") if isinstance(cache.get("payload"), dict) else None
    fetched_at = int(cache.get("fetched_at") or 0)
    now = int(time.time())
    fresh = payload and fetched_at and (now - fetched_at) <= config["cache_seconds"]

    if not twelve_data_api_key() and not force_refresh:
        return collect_market_pulse(), 200

    if payload and fresh and not force_refresh:
        cloned = _json_clone(payload)
        cloned["from_cache"] = True
        cloned["stale"] = False
        return cloned, 200

    if payload and not force_refresh:
        schedule_market_pulse_refresh()
        cloned = _json_clone(payload)
        cloned["from_cache"] = True
        cloned["stale"] = True
        cloned["warming_up"] = True
        return cloned, 200

    if not payload and not force_refresh:
        schedule_market_pulse_refresh()
        return warming_market_pulse_payload(now), 200

    try:
        return refresh_market_pulse_cache(), 200
    except Exception as error:
        if payload:
            cloned = _json_clone(payload)
            cloned["from_cache"] = True
            cloned["stale"] = True
            cloned["errors"] = list(cloned.get("errors") or [])
            cloned["errors"].append({"source": "market_pulse", "error": str(error)})
            return cloned, 200
        return {
            "configured": bool(twelve_data_api_key()),
            "fetched_at": now,
            "from_cache": False,
            "stale": False,
            "instruments": [],
            "mood": {
                "score": None,
                "label": "Unavailable",
                "tone": "neutral",
                "summary": "Market analysis is unavailable right now.",
                "notes": [],
            },
            "strategy": [],
            "errors": [{"source": "market_pulse", "error": str(error)}],
        }, 502


def market_pulse_refresher_loop():
    startup_delay = load_market_pulse_config()["startup_delay_seconds"]
    if startup_delay > 0:
        time.sleep(startup_delay)
    while True:
        config = load_market_pulse_config()
        cache = load_market_pulse_cache()
        payload = cache.get("payload") if isinstance(cache.get("payload"), dict) else None
        fetched_at = int(cache.get("fetched_at") or 0)
        stale = not payload or not fetched_at or (int(time.time()) - fetched_at) > config["cache_seconds"]
        if stale:
            try:
                refresh_market_pulse_cache()
            except Exception as error:
                print(f"[MarketPulse] refresh failed: {error}", flush=True)
        time.sleep(max(300, config["cache_seconds"]))


def start_market_pulse_refresher():
    global REFRESH_THREAD_STARTED
    if REFRESH_THREAD_STARTED:
        return
    REFRESH_THREAD_STARTED = True
    worker = threading.Thread(
        target=market_pulse_refresher_loop,
        name="market-pulse-refresher",
        daemon=True,
    )
    worker.start()
