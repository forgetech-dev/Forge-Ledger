import json
import os
import hashlib
import threading
import time
import uuid
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory

import plaid
from plaid.api import plaid_api
from plaid.model.country_code import CountryCode
from plaid.model.products import Products
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
from plaid.model.transactions_sync_request import TransactionsSyncRequest

try:
    from snaptrade_client import SnapTrade
    from snaptrade_client.exceptions import ApiException as SnapTradeApiException
except ImportError:
    SnapTrade = None
    SnapTradeApiException = Exception

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ---- Plaid ----------------------------------------------------------------
PLAID_CLIENT_ID = os.getenv("PLAID_CLIENT_ID")
PLAID_ENV = os.getenv("PLAID_ENV", "production")
if PLAID_ENV == "production":
    PLAID_SECRET = os.getenv("PLAID_PRODUCTION_SECRET")
    host = plaid.Environment.Production
else:
    PLAID_SECRET = os.getenv("PLAID_SANDBOX_SECRET")
    host = plaid.Environment.Sandbox

plaid_client = plaid_api.PlaidApi(
    plaid.ApiClient(
        plaid.Configuration(
            host=host,
            api_key={
                "clientId": PLAID_CLIENT_ID,
                "secret": PLAID_SECRET,
                "plaidVersion": "2020-09-14",
            },
        )
    )
)

# ---- SnapTrade ------------------------------------------------------------
SNAPTRADE_CLIENT_ID = os.getenv("SNAPTRADE_CLIENT_ID")
SNAPTRADE_CONSUMER_KEY = os.getenv("SNAPTRADE_CONSUMER_KEY")
snaptrade = None
if SnapTrade and SNAPTRADE_CLIENT_ID and SNAPTRADE_CONSUMER_KEY:
    snaptrade = SnapTrade(
        consumer_key=SNAPTRADE_CONSUMER_KEY,
        client_id=SNAPTRADE_CLIENT_ID,
    )

# ---- Local persistence ----------------------------------------------------
DEMO_DIR = Path(__file__).resolve().parent
TOKENS_FILE = DEMO_DIR / "tokens.json"
SNAPTRADE_USER_FILE = DEMO_DIR / "snaptrade_user.json"
BALANCE_CACHE_FILE = DEMO_DIR / "balances_cache.json"
HIDDEN_ACCOUNTS_FILE = DEMO_DIR / "hidden_accounts.json"
NET_WORTH_HISTORY_FILE = DEMO_DIR / "net_worth_history.json"
TRANSACTIONS_CACHE_FILE = DEMO_DIR / "transactions_cache.json"
INVESTMENTS_CACHE_FILE = DEMO_DIR / "investments_cache.json"
INVESTMENT_HISTORY_FILE = DEMO_DIR / "investment_history.json"
BALANCE_REFRESH_SECONDS = int(os.getenv("BALANCE_REFRESH_SECONDS", "3600"))
TRANSACTION_LIMIT = int(os.getenv("TRANSACTION_LIMIT", "500"))
BALANCE_CACHE_LOCK = threading.Lock()
TRANSACTIONS_CACHE_LOCK = threading.Lock()
INVESTMENTS_CACHE_LOCK = threading.Lock()
NET_WORTH_HISTORY_LOCK = threading.Lock()
INVESTMENT_HISTORY_LOCK = threading.Lock()
BALANCE_CACHE = None
TRANSACTIONS_CACHE = None
INVESTMENTS_CACHE = None


def _read_json(path, default):
    if not path.exists():
        return default
    txt = path.read_text()
    return json.loads(txt) if txt.strip() else default


def _write_json(path, data):
    path.write_text(json.dumps(data, indent=2))


def _json_clone(data):
    return json.loads(json.dumps(data))


def _json_or_empty(raw):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}


def load_tokens():
    return _read_json(TOKENS_FILE, [])


def save_tokens(tokens):
    _write_json(TOKENS_FILE, tokens)


def load_snaptrade_user():
    return _read_json(SNAPTRADE_USER_FILE, None)


def save_snaptrade_user(data):
    _write_json(SNAPTRADE_USER_FILE, data)


def load_hidden_accounts():
    data = _read_json(HIDDEN_ACCOUNTS_FILE, {"hidden": {}})
    if isinstance(data, list):
        return {"hidden": {key: {"hidden_at": None} for key in data}}
    if "hidden" not in data or not isinstance(data["hidden"], dict):
        return {"hidden": {}}
    return data


def save_hidden_accounts(data):
    _write_json(HIDDEN_ACCOUNTS_FILE, data)


def hidden_account_keys():
    return set(load_hidden_accounts()["hidden"].keys())


def load_net_worth_history():
    data = _read_json(NET_WORTH_HISTORY_FILE, [])
    if isinstance(data, dict):
        data = data.get("history", [])
    if not isinstance(data, list):
        return []
    valid = []
    for point in data:
        if not isinstance(point, dict):
            continue
        if "date" not in point or "net_worth" not in point:
            continue
        valid.append(
            {
                "date": str(point["date"]),
                "net_worth": float(point["net_worth"]),
                "recorded_at": point.get("recorded_at"),
            }
        )
    return sorted(valid, key=lambda point: point["date"])


def save_net_worth_history(history):
    _write_json(NET_WORTH_HISTORY_FILE, history)


def record_net_worth_history(payload):
    today = date.today().isoformat()
    point = {
        "date": today,
        "net_worth": round(float(payload.get("net_worth") or 0), 2),
        "recorded_at": int(payload.get("fetched_at") or time.time()),
    }
    with NET_WORTH_HISTORY_LOCK:
        history = [p for p in load_net_worth_history() if p["date"] != today]
        history.append(point)
        history = sorted(history, key=lambda p: p["date"])
        save_net_worth_history(history)
    return history


def load_investment_history():
    data = _read_json(INVESTMENT_HISTORY_FILE, [])
    if isinstance(data, dict):
        data = data.get("history", [])
    if not isinstance(data, list):
        return []
    valid = []
    for point in data:
        if not isinstance(point, dict):
            continue
        if "date" not in point or "total_value" not in point:
            continue
        valid.append(
            {
                "date": str(point["date"]),
                "total_value": float(point["total_value"]),
                "recorded_at": point.get("recorded_at"),
            }
        )
    return sorted(valid, key=lambda point: point["date"])


def save_investment_history(history):
    _write_json(INVESTMENT_HISTORY_FILE, history)


def record_investment_history(payload):
    today = date.today().isoformat()
    point = {
        "date": today,
        "total_value": round(float(payload.get("total_value") or 0), 2),
        "recorded_at": int(payload.get("fetched_at") or time.time()),
    }
    with INVESTMENT_HISTORY_LOCK:
        history = [p for p in load_investment_history() if p["date"] != today]
        history.append(point)
        history = sorted(history, key=lambda p: p["date"])
        save_investment_history(history)
    return history


def load_balance_cache():
    global BALANCE_CACHE
    cached = _read_json(BALANCE_CACHE_FILE, None)
    with BALANCE_CACHE_LOCK:
        BALANCE_CACHE = cached
    return cached


def load_transactions_cache():
    global TRANSACTIONS_CACHE
    cached = _read_json(TRANSACTIONS_CACHE_FILE, None)
    with TRANSACTIONS_CACHE_LOCK:
        TRANSACTIONS_CACHE = cached
    return cached


def load_investments_cache():
    global INVESTMENTS_CACHE
    cached = _read_json(INVESTMENTS_CACHE_FILE, None)
    with INVESTMENTS_CACHE_LOCK:
        INVESTMENTS_CACHE = cached
    return cached


def get_balance_cache():
    with BALANCE_CACHE_LOCK:
        return _json_clone(BALANCE_CACHE) if BALANCE_CACHE is not None else None


def get_transactions_cache():
    with TRANSACTIONS_CACHE_LOCK:
        return _json_clone(TRANSACTIONS_CACHE) if TRANSACTIONS_CACHE is not None else None


def get_investments_cache():
    with INVESTMENTS_CACHE_LOCK:
        return _json_clone(INVESTMENTS_CACHE) if INVESTMENTS_CACHE is not None else None


def save_balance_cache(payload):
    global BALANCE_CACHE
    cached = _json_clone(payload)
    with BALANCE_CACHE_LOCK:
        BALANCE_CACHE = cached
    _write_json(BALANCE_CACHE_FILE, cached)
    return cached


def save_transactions_cache(payload):
    global TRANSACTIONS_CACHE
    cached = _json_clone(payload)
    with TRANSACTIONS_CACHE_LOCK:
        TRANSACTIONS_CACHE = cached
    _write_json(TRANSACTIONS_CACHE_FILE, cached)
    return cached


def save_investments_cache(payload):
    global INVESTMENTS_CACHE
    cached = _json_clone(payload)
    with INVESTMENTS_CACHE_LOCK:
        INVESTMENTS_CACHE = cached
    _write_json(INVESTMENTS_CACHE_FILE, cached)
    return cached


def empty_balance_payload():
    return {
        "groups": {"deposit": [], "investment": [], "debt": [], "other": []},
        "totals": {"deposit": 0.0, "investment": 0.0, "debt": 0.0, "other": 0.0},
        "net_worth": 0.0,
        "errors": [],
        "fetched_at": None,
    }


def empty_transactions_payload():
    return {"transactions": [], "errors": [], "fetched_at": None}


def empty_investments_payload():
    return {
        "accounts": [],
        "allocation": [],
        "holdings": [],
        "total_value": 0.0,
        "history": [],
        "warnings": [],
        "errors": [],
        "fetched_at": None,
    }


def with_net_worth_history(payload):
    enriched = _json_clone(payload)
    with NET_WORTH_HISTORY_LOCK:
        enriched["history"] = load_net_worth_history()
    return enriched


def with_investment_history(payload):
    enriched = normalized_investment_payload(payload)
    with INVESTMENT_HISTORY_LOCK:
        enriched["history"] = load_investment_history()
    return enriched


def account_key(source, *parts):
    raw = "|".join(str(part or "") for part in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"{source}:{digest}"


def plaid_account_key(account):
    return account_key("plaid", account.get("account_id"))


def snaptrade_account_key(account, institution):
    meta = account.get("meta") or {}
    brokerage = account.get("brokerage") or {}
    stable_parts = [
        account.get("id"),
        account.get("account_id"),
        account.get("brokerage_authorization"),
        account.get("number"),
        institution,
        brokerage.get("name"),
        account.get("name"),
        meta.get("type"),
    ]
    return account_key("snaptrade", *stable_parts)


def summarize_account(account):
    return {
        "source": account.get("source"),
        "institution": account.get("institution"),
        "name": account.get("name"),
        "mask": account.get("mask"),
        "type": account.get("type"),
        "subtype": account.get("subtype"),
    }


def find_cached_account(account_key_value):
    cached = get_balance_cache()
    if not cached:
        return None
    for accounts in (cached.get("groups") or {}).values():
        for account in accounts:
            if account.get("account_key") == account_key_value:
                return account
    return None


def recompute_balance_payload(payload):
    totals = {"deposit": 0.0, "investment": 0.0, "debt": 0.0, "other": 0.0}
    for key, accounts in (payload.get("groups") or {}).items():
        totals[key] = sum(float(account.get("current") or 0) for account in accounts)
    payload["totals"] = totals
    payload["net_worth"] = totals["deposit"] + totals["investment"] - totals["debt"]
    return payload


def apply_hidden_accounts(payload, hidden_keys=None):
    hidden = hidden_keys if hidden_keys is not None else hidden_account_keys()
    filtered = _json_clone(payload)
    for key, accounts in (filtered.get("groups") or {}).items():
        filtered["groups"][key] = [
            account
            for account in accounts
            if account.get("account_key") not in hidden
        ]
    filtered["hidden_account_count"] = len(hidden)
    return recompute_balance_payload(filtered)


def _body(resp):
    for attr in ("body", "data"):
        if hasattr(resp, attr):
            return getattr(resp, attr)
    return resp


def _list_snaptrade_users():
    try:
        resp = snaptrade.authentication.list_snap_trade_users()
        return _body(resp) or []
    except Exception as e:
        print(f"[SnapTrade] list_users failed: {e}", flush=True)
        return []


def _cleanup_snaptrade_users():
    users = _list_snaptrade_users()
    print(f"[SnapTrade] existing users before cleanup: {users}", flush=True)
    deleted = 0
    for u in users:
        uid = u if isinstance(u, str) else (u.get("userId") or u.get("user_id"))
        if not uid:
            continue
        try:
            snaptrade.authentication.delete_snap_trade_user(user_id=uid)
            print(f"[SnapTrade] delete requested for {uid}", flush=True)
            deleted += 1
        except Exception as e:
            print(f"[SnapTrade] delete failed for {uid}: {e}", flush=True)
    return deleted


def ensure_snaptrade_user():
    user = load_snaptrade_user()
    if user and user.get("user_secret"):
        return user["user_id"], user["user_secret"]

    user_id = f"forge-{uuid.uuid4().hex[:12]}"

    def _register():
        return snaptrade.authentication.register_snap_trade_user(user_id=user_id)

    try:
        resp = _register()
    except SnapTradeApiException as e:
        body_str = str(getattr(e, "body", ""))
        if "1012" not in body_str and "can only register one user" not in body_str:
            raise

        deleted = _cleanup_snaptrade_users()
        if deleted == 0:
            raise RuntimeError(
                "SnapTrade personal key already has a user, but list_snap_trade_users "
                "returned none. Delete the user manually from the SnapTrade dashboard "
                "(https://dashboard.snaptrade.com/) and retry."
            )

        # SnapTrade user deletion is async. Poll until the list is empty, then retry.
        resp = None
        for attempt in range(8):
            time.sleep(2)
            remaining = _list_snaptrade_users()
            print(f"[SnapTrade] waiting for delete; remaining={remaining}", flush=True)
            if not remaining:
                try:
                    resp = _register()
                    break
                except SnapTradeApiException as e2:
                    body2 = str(getattr(e2, "body", ""))
                    if "1012" in body2 or "can only register one user" in body2:
                        continue
                    raise
        if resp is None:
            raise RuntimeError(
                "SnapTrade still reports an existing user after cleanup attempts. "
                "Try again in a minute, or delete the user via the SnapTrade dashboard."
            )

    body = _body(resp)
    stored = {
        "user_id": body.get("userId") or user_id,
        "user_secret": body.get("userSecret"),
    }
    save_snaptrade_user(stored)
    return stored["user_id"], stored["user_secret"]


def categorize(type_str: str) -> str:
    t = (type_str or "").lower()
    if t == "depository":
        return "deposit"
    if t in ("investment", "brokerage"):
        return "investment"
    if t in ("credit", "loan"):
        return "debt"
    return "other"


# ---- App ------------------------------------------------------------------
app = Flask(__name__, static_folder=".", static_url_path="")


@app.route("/")
def home():
    return send_from_directory(".", "index.html")


# ---- Plaid endpoints ------------------------------------------------------
@app.route("/api/create_link_token", methods=["POST"])
def create_link_token():
    req = LinkTokenCreateRequest(
        products=[Products("transactions")],
        additional_consented_products=[Products("investments")],
        client_name="Forge Ledger Demo",
        country_codes=[CountryCode("US")],
        language="en",
        user=LinkTokenCreateRequestUser(client_user_id="demo-user"),
    )
    return jsonify(plaid_client.link_token_create(req).to_dict())


@app.route("/api/plaid/items", methods=["GET"])
def plaid_items():
    return jsonify(
        {
            "items": [
                {
                    "item_id": tok.get("item_id"),
                    "institution_id": tok.get("institution_id"),
                    "institution_name": tok.get("institution_name"),
                    "linked_at": tok.get("linked_at"),
                }
                for tok in load_tokens()
            ]
        }
    )


@app.route("/api/create_update_link_token", methods=["POST"])
def create_update_link_token():
    data = request.get_json(silent=True) or {}
    item_id = data.get("item_id")
    tok = next((item for item in load_tokens() if item.get("item_id") == item_id), None)
    if not tok:
        return jsonify({"error": "Unknown Plaid item_id"}), 404

    req = LinkTokenCreateRequest(
        access_token=tok["access_token"],
        additional_consented_products=[Products("investments")],
        client_name="Forge Ledger Demo",
        country_codes=[CountryCode("US")],
        language="en",
        user=LinkTokenCreateRequestUser(client_user_id="demo-user"),
    )
    return jsonify(plaid_client.link_token_create(req).to_dict())


@app.route("/api/set_access_token", methods=["POST"])
def set_access_token():
    public_token = request.json["public_token"]
    exchange = plaid_client.item_public_token_exchange(
        ItemPublicTokenExchangeRequest(public_token=public_token)
    )
    access_token = exchange["access_token"]
    item_id = exchange["item_id"]

    item_resp = plaid_client.item_get(ItemGetRequest(access_token=access_token)).to_dict()
    inst_id = item_resp["item"].get("institution_id")
    inst_name = "Unknown"
    if inst_id:
        inst_resp = plaid_client.institutions_get_by_id(
            InstitutionsGetByIdRequest(
                institution_id=inst_id,
                country_codes=[CountryCode("US")],
            )
        ).to_dict()
        inst_name = inst_resp["institution"]["name"]

    tokens = load_tokens()
    tokens.append(
        {
            "access_token": access_token,
            "item_id": item_id,
            "institution_id": inst_id,
            "institution_name": inst_name,
            "linked_at": int(time.time()),
        }
    )
    save_tokens(tokens)
    return jsonify({"ok": True, "institution": inst_name})


# ---- SnapTrade endpoints --------------------------------------------------
@app.route("/api/snaptrade/status", methods=["GET"])
def snaptrade_status():
    return jsonify(
        {
            "configured": snaptrade is not None,
            "registered": load_snaptrade_user() is not None,
        }
    )


@app.route("/api/snaptrade/connect", methods=["POST"])
def snaptrade_connect():
    if not snaptrade:
        return jsonify({"error": "SnapTrade not configured"}), 400
    try:
        user_id, user_secret = ensure_snaptrade_user()
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 400
    except SnapTradeApiException as e:
        return jsonify({"error": str(getattr(e, "body", e))}), 400
    resp = snaptrade.authentication.login_snap_trade_user(
        user_id=user_id,
        user_secret=user_secret,
    )
    body = _body(resp)
    return jsonify(
        {"redirect_uri": body.get("redirectURI") or body.get("redirect_uri")}
    )


# ---- Aggregate ------------------------------------------------------------
def _pull_plaid(grouped, totals, errors, hidden_keys):
    for tok in load_tokens():
        try:
            resp = plaid_client.accounts_balance_get(
                AccountsBalanceGetRequest(access_token=tok["access_token"])
            ).to_dict()
        except plaid.ApiException as e:
            errors.append(
                {"source": "plaid", "institution": tok["institution_name"], "error": e.body}
            )
            continue

        for a in resp["accounts"]:
            key = plaid_account_key(a)
            if key in hidden_keys:
                continue
            type_str = str(a["type"])
            cat = categorize(type_str)
            current = a["balances"].get("current") or 0.0
            grouped[cat].append(
                {
                    "account_key": key,
                    "institution": tok["institution_name"],
                    "name": a["name"],
                    "mask": a.get("mask"),
                    "type": type_str,
                    "subtype": str(a.get("subtype") or ""),
                    "current": a["balances"].get("current"),
                    "available": a["balances"].get("available"),
                    "limit": a["balances"].get("limit"),
                    "iso_currency_code": a["balances"].get("iso_currency_code"),
                    "source": "plaid",
                }
            )
            totals[cat] += float(current)


def _pull_snaptrade(grouped, totals, errors, hidden_keys):
    if not snaptrade:
        return
    user = load_snaptrade_user()
    if not user:
        return
    try:
        resp = snaptrade.account_information.list_user_accounts(
            user_id=user["user_id"],
            user_secret=user["user_secret"],
        )
        accounts = _body(resp) or []
    except Exception as e:
        errors.append({"source": "snaptrade", "error": str(e)})
        return

    for a in accounts:
        balance_obj = a.get("balance") or {}
        total = balance_obj.get("total") if isinstance(balance_obj, dict) else None
        if isinstance(total, dict):
            amount = total.get("amount")
            currency = total.get("currency") or "USD"
        else:
            amount = total
            currency = "USD"

        inst = (
            a.get("institution_name")
            or (a.get("brokerage") or {}).get("name")
            or (a.get("meta") or {}).get("institution_name")
            or "Brokerage"
        )
        number = a.get("number") or ""
        mask = number[-4:] if len(number) >= 4 else None
        key = snaptrade_account_key(a, inst)
        if key in hidden_keys:
            continue

        grouped["investment"].append(
            {
                "account_key": key,
                "institution": inst,
                "name": a.get("name") or inst,
                "mask": mask,
                "type": "investment",
                "subtype": (a.get("meta") or {}).get("type") or "brokerage",
                "current": amount,
                "available": None,
                "limit": None,
                "iso_currency_code": currency,
                "source": "snaptrade",
            }
        )
        totals["investment"] += float(amount or 0)


def _dictish(obj):
    if obj is None:
        return obj
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if isinstance(obj, list):
        return [_dictish(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _dictish(value) for key, value in obj.items()}
    return obj


def stock_plan_text(name=None, subtype=None, account=None):
    parts = [name, subtype]
    if isinstance(account, dict):
        parts.extend(
            [
                account.get("name"),
                account.get("subtype"),
                account.get("type"),
                (account.get("meta") or {}).get("type"),
            ]
        )
    return " ".join(str(part or "").lower() for part in parts)


def is_unvested_stock_grant_holding(account, symbol, name, value, quantity):
    text = stock_plan_text(account.get("name"), account.get("subtype"), account)
    holding_text = f"{symbol or ''} {name or ''}".lower()
    return (
        "stock plan" in text
        and ("ffiv" in holding_text or "f5" in holding_text)
        and float(quantity or 0) > 200
        and float(value or 0) > 50000
    )


def is_stock_plan_account(account):
    if not isinstance(account, dict):
        return False
    return "stock plan" in stock_plan_text(
        account.get("name"),
        account.get("subtype") or account.get("type"),
        account,
    )


def is_buying_power_holding(symbol, name, asset_type=None):
    ticker = str(symbol or "").upper()
    text = " ".join(str(part or "").lower() for part in (symbol, name, asset_type))
    cash_symbols = {"USD", "CUR:USD", "FDLXX", "SPAXX"}
    return (
        ticker in cash_symbols
        or ticker.startswith("CUR:")
        or "u s dollar" in text
        or "buying power" in text
        or "money market" in text
        or "treasury only money" in text
        or "government money market" in text
    )


def investment_account_rows(balance_payload):
    accounts = []
    for account in (balance_payload.get("groups") or {}).get("investment", []):
        accounts.append(
            {
                "account_key": account.get("account_key"),
                "institution": account.get("institution"),
                "name": account.get("name"),
                "mask": account.get("mask"),
                "subtype": account.get("subtype") or account.get("type") or "investment",
                "current": account.get("current") or 0,
                "source": account.get("source"),
            }
        )
    accounts.sort(key=lambda a: float(a.get("current") or 0), reverse=True)
    return accounts


def classify_asset_type(security_type, ticker, name):
    text = " ".join(str(part or "").lower() for part in (security_type, ticker, name))
    if "cash" in text or ticker in ("USD", "CUR:USD"):
        return "Cash"
    if "crypto" in text or "bitcoin" in text or "ethereum" in text:
        return "Crypto"
    if "mutual" in text or "money market" in text or "open ended fund" in text or "oef" in text:
        return "Mutual Fund"
    if "etf" in text or "exchange traded" in text:
        return "ETF"
    if "bond" in text or "fixed income" in text:
        return "Other"
    if "equity" in text or "stock" in text or "american depositary receipt" in text:
        return "Equity"
    return "Other"


def add_allocation(allocation_totals, asset_type, value):
    allocation_totals[asset_type] = allocation_totals.get(asset_type, 0.0) + float(value or 0)


def investment_account_match_key(source, institution, account_name):
    return "|".join(str(part or "") for part in (source, institution, account_name))


def investment_account_values(accounts):
    return {
        investment_account_match_key(
            account.get("source"),
            account.get("institution"),
            account.get("name"),
        ): float(account.get("current") or 0)
        for account in accounts
    }


def accounts_from_investable_holdings(accounts, holdings):
    if not holdings:
        return accounts

    totals = {}
    used_keys = set()
    for holding in holdings:
        key = investment_account_match_key(
            holding.get("source"),
            holding.get("institution"),
            holding.get("account_name"),
        )
        totals[key] = totals.get(key, 0.0) + float(holding.get("value") or 0)

    display_accounts = []
    for account in accounts:
        key = investment_account_match_key(
            account.get("source"),
            account.get("institution"),
            account.get("name"),
        )
        value = totals.get(key, 0.0)
        if value <= 0:
            continue
        used_keys.add(key)
        display_accounts.append({**account, "current": value})

    for holding in holdings:
        key = investment_account_match_key(
            holding.get("source"),
            holding.get("institution"),
            holding.get("account_name"),
        )
        if key in used_keys:
            continue
        used_keys.add(key)
        display_accounts.append(
            {
                "account_key": None,
                "institution": holding.get("institution"),
                "name": holding.get("account_name") or holding.get("institution") or "Investment Account",
                "mask": None,
                "subtype": "investment",
                "current": totals.get(key, 0.0),
                "source": holding.get("source"),
            }
        )

    display_accounts.sort(key=lambda account: float(account.get("current") or 0), reverse=True)
    return display_accounts


def normalized_investment_payload(payload):
    enriched = _json_clone(payload)
    balance_payload = get_balance_cache() or empty_balance_payload()
    raw_accounts = investment_account_rows(balance_payload)
    raw_account_by_key = {
        investment_account_match_key(
            account.get("source"),
            account.get("institution"),
            account.get("name"),
        ): account
        for account in raw_accounts
    }
    raw_account_values = investment_account_values(raw_accounts)
    holdings = []
    reconciled_stock_plan_accounts = set()

    for holding in enriched.get("holdings") or []:
        key = investment_account_match_key(
            holding.get("source"),
            holding.get("institution"),
            holding.get("account_name"),
        )
        account = raw_account_by_key.get(key) or {"name": holding.get("account_name")}
        value = float(holding.get("value") or 0)
        quantity = holding.get("quantity")
        symbol = holding.get("symbol")
        name = holding.get("name")

        if is_unvested_stock_grant_holding(account, symbol, name, value, quantity):
            continue
        if is_buying_power_holding(symbol, name, holding.get("asset_type")):
            continue
        if is_stock_plan_account(account):
            current_value = raw_account_values.get(key, 0.0)
            if current_value > 0:
                if key in reconciled_stock_plan_accounts:
                    continue
                holding = {**holding, "value": current_value}
                reconciled_stock_plan_accounts.add(key)

        holdings.append(holding)

    total_value = sum(float(item.get("value") or 0) for item in holdings)
    allocation_totals = {}
    for holding in holdings:
        add_allocation(
            allocation_totals,
            holding.get("asset_type") or "Other",
            holding.get("value") or 0,
        )
    allocation = [
        {
            "asset_type": asset_type,
            "value": value,
            "percent": (value / total_value * 100) if total_value else 0,
        }
        for asset_type, value in allocation_totals.items()
        if value > 0
    ]
    allocation.sort(key=lambda item: item["value"], reverse=True)
    holdings.sort(key=lambda item: float(item.get("value") or 0), reverse=True)

    enriched["holdings"] = holdings
    enriched["allocation"] = allocation
    enriched["accounts"] = accounts_from_investable_holdings(raw_accounts, holdings)
    enriched["total_value"] = total_value or float((balance_payload.get("totals") or {}).get("investment") or 0)
    return enriched


def _text_value(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def _nested_value(obj, *path):
    current = obj
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _first_text(*values):
    for value in values:
        text = _text_value(value)
        if text:
            return text
    return ""


def snaptrade_security_fields(position):
    raw_symbol = position.get("symbol")
    security = position.get("security") if isinstance(position.get("security"), dict) else {}
    symbol_obj = raw_symbol if isinstance(raw_symbol, dict) else {}

    symbol = _first_text(
        position.get("ticker"),
        raw_symbol,
        security.get("symbol"),
        security.get("ticker"),
        symbol_obj.get("raw_symbol"),
        _nested_value(symbol_obj, "symbol", "symbol"),
        _nested_value(symbol_obj, "symbol", "raw_symbol"),
    )
    name = _first_text(
        position.get("name"),
        position.get("description"),
        security.get("name"),
        security.get("description"),
        symbol_obj.get("description"),
        _nested_value(symbol_obj, "symbol", "description"),
        symbol,
        "Holding",
    )
    security_type = _first_text(
        position.get("type"),
        security.get("type"),
        symbol_obj.get("security_type"),
        _nested_value(symbol_obj, "security_type", "description"),
        _nested_value(symbol_obj, "security_type", "code"),
        _nested_value(symbol_obj, "symbol", "type", "description"),
        _nested_value(symbol_obj, "symbol", "type", "code"),
    )
    return symbol, name, security_type


def holding_value_from_snaptrade(position):
    for key in ("market_value", "value", "institution_value"):
        value = position.get(key)
        if isinstance(value, dict):
            value = value.get("amount")
        if value is not None:
            return float(value or 0)

    price = position.get("price") or position.get("average_purchase_price")
    quantity = position.get("units") or position.get("quantity")
    if isinstance(price, dict):
        price = price.get("amount")
    if quantity is not None and price is not None:
        return float(quantity or 0) * float(price or 0)
    return 0.0


def _pull_plaid_holdings(
    holdings,
    allocation_totals,
    errors,
    warnings,
    hidden_keys,
    investment_institutions,
    account_values,
):
    reconciled_stock_plan_accounts = set()
    for tok in load_tokens():
        institution = tok["institution_name"]
        if investment_institutions and institution not in investment_institutions:
            continue
        try:
            resp = plaid_client.investments_holdings_get(
                InvestmentsHoldingsGetRequest(access_token=tok["access_token"])
            ).to_dict()
        except plaid.ApiException as e:
            body = _json_or_empty(e.body)
            if body.get("error_code") == "ADDITIONAL_CONSENT_REQUIRED":
                warnings.append(
                    {
                        "source": "plaid",
                        "institution": institution,
                        "code": body.get("error_code"),
                        "message": (
                            "Investment holdings were not granted for this Plaid connection. "
                            "Relink or use Plaid update mode with investments consent to show positions."
                        ),
                    }
                )
                continue
            errors.append(
                {
                    "source": "plaid",
                    "institution": institution,
                    "error": body.get("error_message") or e.body,
                }
            )
            continue

        securities = {s.get("security_id"): s for s in resp.get("securities", [])}
        accounts = {a.get("account_id"): a for a in resp.get("accounts", [])}
        for holding in resp.get("holdings", []):
            account_id = holding.get("account_id")
            account = accounts.get(account_id) or {}
            if account_key("plaid", account_id) in hidden_keys:
                continue
            security = securities.get(holding.get("security_id")) or {}
            ticker = security.get("ticker_symbol") or security.get("proxy_security_id") or ""
            name = security.get("name") or ticker or "Holding"
            value = float(holding.get("institution_value") or 0)
            quantity = holding.get("quantity")
            if is_unvested_stock_grant_holding(account, ticker, name, value, quantity):
                continue
            holding_account_key = investment_account_match_key(
                "plaid",
                institution,
                account.get("name"),
            )
            if is_stock_plan_account(account):
                current_value = account_values.get(holding_account_key, 0.0)
                if current_value > 0:
                    if holding_account_key in reconciled_stock_plan_accounts:
                        continue
                    value = current_value
                    reconciled_stock_plan_accounts.add(holding_account_key)
            asset_type = classify_asset_type(security.get("type"), ticker, name)
            if is_buying_power_holding(ticker, name, asset_type):
                continue
            add_allocation(allocation_totals, asset_type, value)
            holdings.append(
                {
                    "symbol": ticker or name[:5].upper(),
                    "name": name,
                    "value": value,
                    "quantity": quantity,
                    "price": holding.get("institution_price"),
                    "asset_type": asset_type,
                    "institution": institution,
                    "account_name": account.get("name"),
                    "source": "plaid",
                }
            )


def _snaptrade_positions_from_account(account, user):
    account_id = account.get("id") or account.get("account_id") or account.get("number")
    candidates = [
        ("get_user_account_positions", {"account_id": account_id}),
        ("list_user_account_positions", {"account_id": account_id}),
        ("get_user_holdings", {"account_id": account_id}),
    ]
    for method_name, extra in candidates:
        method = getattr(snaptrade.account_information, method_name, None)
        if not method or not account_id:
            continue
        try:
            resp = method(
                user_id=user["user_id"],
                user_secret=user["user_secret"],
                **extra,
            )
            data = _dictish(_body(resp))
            if isinstance(data, dict):
                data = data.get("positions") or data.get("holdings") or data.get("data") or []
            return data or []
        except Exception:
            continue
    return account.get("positions") or account.get("holdings") or []


def _pull_snaptrade_holdings(holdings, allocation_totals, errors, hidden_keys):
    if not snaptrade:
        return
    user = load_snaptrade_user()
    if not user:
        return
    try:
        resp = snaptrade.account_information.list_user_accounts(
            user_id=user["user_id"],
            user_secret=user["user_secret"],
        )
        accounts = _dictish(_body(resp)) or []
    except Exception as e:
        errors.append({"source": "snaptrade", "error": str(e)})
        return

    for account in accounts:
        inst = (
            account.get("institution_name")
            or (account.get("brokerage") or {}).get("name")
            or (account.get("meta") or {}).get("institution_name")
            or "Brokerage"
        )
        if snaptrade_account_key(account, inst) in hidden_keys:
            continue
        positions = _dictish(_snaptrade_positions_from_account(account, user)) or []
        if not positions:
            balance_obj = account.get("balance") or {}
            total = balance_obj.get("total") if isinstance(balance_obj, dict) else None
            value = total.get("amount") if isinstance(total, dict) else total
            value = float(value or 0)
            add_allocation(allocation_totals, "Other", value)
            holdings.append(
                {
                    "symbol": "ACCT",
                    "name": account.get("name") or inst,
                    "value": value,
                    "quantity": None,
                    "price": None,
                    "asset_type": "Other",
                    "institution": inst,
                    "account_name": account.get("name") or inst,
                    "source": "snaptrade",
                }
            )
            continue

        for position in positions:
            symbol, name, security_type = snaptrade_security_fields(position)
            value = holding_value_from_snaptrade(position)
            quantity = position.get("units") or position.get("quantity")
            if is_unvested_stock_grant_holding(account, symbol, name, value, quantity):
                continue
            asset_type = classify_asset_type(security_type, symbol, name)
            if is_buying_power_holding(symbol, name, asset_type):
                continue
            add_allocation(allocation_totals, asset_type, value)
            holdings.append(
                {
                    "symbol": symbol or name[:5].upper(),
                    "name": name,
                    "value": value,
                    "quantity": quantity,
                    "price": position.get("price"),
                    "asset_type": asset_type,
                    "institution": inst,
                    "account_name": account.get("name") or inst,
                    "source": "snaptrade",
                }
            )


def collect_investments():
    balance_payload = get_balance_cache() or empty_balance_payload()
    accounts = investment_account_rows(balance_payload)
    holdings = []
    allocation_totals = {}
    errors = []
    warnings = []
    hidden_keys = hidden_account_keys()
    account_values = investment_account_values(accounts)
    plaid_investment_institutions = {
        account.get("institution")
        for account in accounts
        if account.get("source") == "plaid" and account.get("institution")
    }

    _pull_plaid_holdings(
        holdings,
        allocation_totals,
        errors,
        warnings,
        hidden_keys,
        plaid_investment_institutions,
        account_values,
    )
    _pull_snaptrade_holdings(holdings, allocation_totals, errors, hidden_keys)

    if not holdings:
        for account in accounts:
            value = float(account.get("current") or 0)
            add_allocation(allocation_totals, "Other", value)
            holdings.append(
                {
                    "symbol": account.get("source", "INV").upper()[:5],
                    "name": account.get("name") or "Investment Account",
                    "value": value,
                    "quantity": None,
                    "price": None,
                    "asset_type": "Other",
                    "institution": account.get("institution"),
                    "account_name": account.get("name"),
                    "source": account.get("source"),
                }
            )

    total_value = sum(float(item.get("value") or 0) for item in holdings)
    allocation = [
        {
            "asset_type": asset_type,
            "value": value,
            "percent": (value / total_value * 100) if total_value else 0,
        }
        for asset_type, value in allocation_totals.items()
        if value > 0
    ]
    allocation.sort(key=lambda item: item["value"], reverse=True)
    holdings.sort(key=lambda item: float(item.get("value") or 0), reverse=True)
    accounts = accounts_from_investable_holdings(accounts, holdings)

    return {
        "accounts": accounts,
        "allocation": allocation,
        "holdings": holdings,
        "total_value": total_value or float((balance_payload.get("totals") or {}).get("investment") or 0),
        "warnings": warnings,
        "errors": errors,
        "fetched_at": int(time.time()),
    }


def refresh_investments_cache():
    payload = collect_investments()
    payload["history"] = record_investment_history(payload)
    save_investments_cache(payload)
    print(
        f"[Investments] refreshed at {payload['fetched_at']} "
        f"with {len(payload['holdings'])} holdings",
        flush=True,
    )
    return payload


def record_investments_refresh_failure(error):
    cached = get_investments_cache() or empty_investments_payload()
    cached["refresh_failed_at"] = int(time.time())
    cached["errors"] = list(cached.get("errors") or [])
    cached["errors"].append({"source": "server", "error": str(error)})
    save_investments_cache(cached)
    print(f"[Investments] refresh failed: {error}", flush=True)
    return cached


def _account_map_for_token(access_token):
    resp = plaid_client.accounts_get(
        AccountsGetRequest(access_token=access_token)
    ).to_dict()
    accounts = {}
    for account in resp.get("accounts", []):
        account_type = str(account.get("type") or "").lower()
        if account_type not in ("depository", "credit"):
            continue
        accounts[account["account_id"]] = {
            "name": account.get("name"),
            "mask": account.get("mask"),
            "type": account_type,
            "subtype": str(account.get("subtype") or ""),
        }
    return accounts


def _transaction_category(transaction):
    pfc = transaction.get("personal_finance_category") or {}
    detailed = pfc.get("detailed")
    primary = pfc.get("primary")
    if detailed:
        return str(detailed).replace("_", " ").title()
    if primary:
        return str(primary).replace("_", " ").title()
    categories = transaction.get("category") or []
    if categories:
        return str(categories[0])
    return "Uncategorized"


def _pull_plaid_transactions(transactions, errors):
    for tok in load_tokens():
        try:
            account_map = _account_map_for_token(tok["access_token"])
            if not account_map:
                continue

            cursor = ""
            has_more = True
            added = []
            while has_more:
                req = TransactionsSyncRequest(
                    access_token=tok["access_token"],
                    cursor=cursor,
                    count=100,
                )
                resp = plaid_client.transactions_sync(req).to_dict()
                added.extend(resp.get("added") or [])
                cursor = resp.get("next_cursor") or ""
                has_more = bool(resp.get("has_more"))
        except plaid.ApiException as e:
            errors.append(
                {
                    "source": "plaid",
                    "institution": tok["institution_name"],
                    "error": e.body,
                }
            )
            continue

        for transaction in added:
            account_id = transaction.get("account_id")
            account = account_map.get(account_id)
            if not account:
                continue
            amount = float(transaction.get("amount") or 0)
            date_str = str(transaction.get("date") or "")
            pfc = transaction.get("personal_finance_category") or {}
            transactions.append(
                {
                    "id": transaction.get("transaction_id"),
                    "date": date_str,
                    "name": transaction.get("merchant_name")
                    or transaction.get("name")
                    or "Transaction",
                    "amount": amount,
                    "pending": bool(transaction.get("pending")),
                    "category": _transaction_category(transaction),
                    "category_primary": pfc.get("primary"),
                    "category_detailed": pfc.get("detailed"),
                    "institution": tok["institution_name"],
                    "account_name": account["name"],
                    "account_mask": account["mask"],
                    "account_type": account["type"],
                    "account_subtype": account["subtype"],
                    "iso_currency_code": transaction.get("iso_currency_code") or "USD",
                    "payment_channel": transaction.get("payment_channel"),
                }
            )


def collect_transactions():
    transactions = []
    errors = []
    _pull_plaid_transactions(transactions, errors)
    transactions.sort(key=lambda t: (t.get("date") or "", t.get("name") or ""), reverse=True)
    if TRANSACTION_LIMIT > 0:
        transactions = transactions[:TRANSACTION_LIMIT]
    return {
        "transactions": transactions,
        "errors": errors,
        "fetched_at": int(time.time()),
    }


def refresh_transactions_cache():
    payload = collect_transactions()
    save_transactions_cache(payload)
    print(
        f"[Transactions] refreshed at {payload['fetched_at']} "
        f"with {len(payload['transactions'])} transactions",
        flush=True,
    )
    return payload


def record_transactions_refresh_failure(error):
    cached = get_transactions_cache() or empty_transactions_payload()
    cached["refresh_failed_at"] = int(time.time())
    cached["errors"] = list(cached.get("errors") or [])
    cached["errors"].append({"source": "server", "error": str(error)})
    save_transactions_cache(cached)
    print(f"[Transactions] refresh failed: {error}", flush=True)
    return cached


def collect_balances():
    grouped = {"deposit": [], "investment": [], "debt": [], "other": []}
    totals = {"deposit": 0.0, "investment": 0.0, "debt": 0.0, "other": 0.0}
    errors = []
    hidden_keys = hidden_account_keys()

    _pull_plaid(grouped, totals, errors, hidden_keys)
    _pull_snaptrade(grouped, totals, errors, hidden_keys)

    net_worth = totals["deposit"] + totals["investment"] - totals["debt"]

    return {
        "groups": grouped,
        "totals": totals,
        "net_worth": net_worth,
        "errors": errors,
        "fetched_at": int(time.time()),
        "hidden_account_count": len(hidden_keys),
    }


def refresh_balance_cache():
    payload = collect_balances()
    save_balance_cache(payload)
    history = record_net_worth_history(payload)
    payload["history"] = history
    print(
        f"[Balances] refreshed at {payload['fetched_at']} "
        f"with {sum(len(v) for v in payload['groups'].values())} accounts",
        flush=True,
    )
    return payload


def record_balance_refresh_failure(error):
    cached = get_balance_cache() or empty_balance_payload()
    cached["refresh_failed_at"] = int(time.time())
    cached["errors"] = list(cached.get("errors") or [])
    cached["errors"].append({"source": "server", "error": str(error)})
    save_balance_cache(cached)
    print(f"[Balances] refresh failed: {error}", flush=True)
    return cached


def balance_refresher_loop():
    while True:
        try:
            refresh_balance_cache()
        except Exception as e:
            record_balance_refresh_failure(e)
        try:
            refresh_transactions_cache()
        except Exception as e:
            record_transactions_refresh_failure(e)
        try:
            refresh_investments_cache()
        except Exception as e:
            record_investments_refresh_failure(e)
        time.sleep(BALANCE_REFRESH_SECONDS)


def start_balance_refresher():
    load_balance_cache()
    load_transactions_cache()
    load_investments_cache()
    worker = threading.Thread(
        target=balance_refresher_loop,
        name="balance-refresher",
        daemon=True,
    )
    worker.start()


@app.route("/api/balances", methods=["GET"])
def balances():
    force_refresh = request.args.get("refresh", "").lower() in ("1", "true", "yes")

    if force_refresh:
        try:
            payload = refresh_balance_cache()
        except Exception as e:
            payload = record_balance_refresh_failure(e)
            payload["from_cache"] = True
            return jsonify(with_net_worth_history(payload)), 502

        payload = _json_clone(payload)
        payload["from_cache"] = False
        return jsonify(payload)

    cached = get_balance_cache()
    if cached is None:
        cached = empty_balance_payload()
        cached["errors"].append(
            {
                "source": "cache",
                "error": "Balance cache is warming up. Try refresh in a moment.",
            }
        )

    cached["from_cache"] = True
    return jsonify(with_net_worth_history(cached))


@app.route("/api/transactions", methods=["GET"])
def transactions():
    force_refresh = request.args.get("refresh", "").lower() in ("1", "true", "yes")

    if force_refresh:
        try:
            payload = refresh_transactions_cache()
        except Exception as e:
            payload = record_transactions_refresh_failure(e)
            payload["from_cache"] = True
            return jsonify(payload), 502

        payload = _json_clone(payload)
        payload["from_cache"] = False
        return jsonify(payload)

    cached = get_transactions_cache()
    if cached is None:
        cached = empty_transactions_payload()
        cached["errors"].append(
            {
                "source": "cache",
                "error": "Transaction cache is warming up. Try refresh in a moment.",
            }
        )

    cached["from_cache"] = True
    return jsonify(cached)


@app.route("/api/investments", methods=["GET"])
def investments():
    force_refresh = request.args.get("refresh", "").lower() in ("1", "true", "yes")

    if force_refresh:
        try:
            payload = refresh_investments_cache()
        except Exception as e:
            payload = record_investments_refresh_failure(e)
            payload["from_cache"] = True
            return jsonify(with_investment_history(payload)), 502

        payload = _json_clone(payload)
        payload["from_cache"] = False
        return jsonify(payload)

    cached = get_investments_cache()
    if cached is None:
        cached = empty_investments_payload()
        cached["errors"].append(
            {
                "source": "cache",
                "error": "Investment cache is warming up. Try refresh in a moment.",
            }
        )

    cached["from_cache"] = True
    return jsonify(with_investment_history(cached))


@app.route("/api/accounts/hide", methods=["POST"])
def hide_account():
    data = request.get_json(silent=True) or {}
    key = data.get("account_key")
    if not key or not isinstance(key, str):
        return jsonify({"error": "account_key is required"}), 400

    account = find_cached_account(key) or data
    hidden = load_hidden_accounts()
    hidden["hidden"][key] = {
        **summarize_account(account),
        "hidden_at": int(time.time()),
    }
    save_hidden_accounts(hidden)

    cached = get_balance_cache()
    if cached is not None:
        cached = apply_hidden_accounts(cached, set(hidden["hidden"].keys()))
        save_balance_cache(cached)
        record_net_worth_history(cached)

    return jsonify(
        {
            "ok": True,
            "hidden_account": hidden["hidden"][key],
            "hidden_account_count": len(hidden["hidden"]),
        }
    )


@app.route("/api/reset", methods=["POST"])
def reset():
    save_tokens([])
    if SNAPTRADE_USER_FILE.exists():
        SNAPTRADE_USER_FILE.unlink()
    save_hidden_accounts({"hidden": {}})
    with NET_WORTH_HISTORY_LOCK:
        save_net_worth_history([])
    with INVESTMENT_HISTORY_LOCK:
        save_investment_history([])
    save_transactions_cache(empty_transactions_payload())
    save_investments_cache(empty_investments_payload())
    save_balance_cache(empty_balance_payload())
    return jsonify({"ok": True})


@app.errorhandler(plaid.ApiException)
def handle_plaid_error(e):
    return jsonify({"error": e.body}), e.status


if __name__ == "__main__":
    start_balance_refresher()
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        debug=True,
        use_reloader=False,
    )
