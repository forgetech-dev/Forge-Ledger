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
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest

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
BALANCE_REFRESH_SECONDS = int(os.getenv("BALANCE_REFRESH_SECONDS", "3600"))
BALANCE_CACHE_LOCK = threading.Lock()
NET_WORTH_HISTORY_LOCK = threading.Lock()
BALANCE_CACHE = None


def _read_json(path, default):
    if not path.exists():
        return default
    txt = path.read_text()
    return json.loads(txt) if txt.strip() else default


def _write_json(path, data):
    path.write_text(json.dumps(data, indent=2))


def _json_clone(data):
    return json.loads(json.dumps(data))


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


def load_balance_cache():
    global BALANCE_CACHE
    cached = _read_json(BALANCE_CACHE_FILE, None)
    with BALANCE_CACHE_LOCK:
        BALANCE_CACHE = cached
    return cached


def get_balance_cache():
    with BALANCE_CACHE_LOCK:
        return _json_clone(BALANCE_CACHE) if BALANCE_CACHE is not None else None


def save_balance_cache(payload):
    global BALANCE_CACHE
    cached = _json_clone(payload)
    with BALANCE_CACHE_LOCK:
        BALANCE_CACHE = cached
    _write_json(BALANCE_CACHE_FILE, cached)
    return cached


def empty_balance_payload():
    return {
        "groups": {"deposit": [], "investment": [], "debt": [], "other": []},
        "totals": {"deposit": 0.0, "investment": 0.0, "debt": 0.0, "other": 0.0},
        "net_worth": 0.0,
        "errors": [],
        "fetched_at": None,
    }


def with_net_worth_history(payload):
    enriched = _json_clone(payload)
    with NET_WORTH_HISTORY_LOCK:
        enriched["history"] = load_net_worth_history()
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
        time.sleep(BALANCE_REFRESH_SECONDS)


def start_balance_refresher():
    load_balance_cache()
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
