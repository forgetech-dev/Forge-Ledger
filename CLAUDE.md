# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Run / develop

The app is the `demo/` directory: a single Flask backend (`server.py`) that serves a single-page frontend (`index.html`) at `/`, plus JSON files for local persistence.

```bash
pip install -r demo/requirements.txt          # flask, plaid-python, python-dotenv, snaptrade-python-sdk
python demo/server.py                         # http://localhost:8000  (PORT overrides)
```

`server.py` loads `.env` from the **repo root** (`load_dotenv(Path(__file__).resolve().parent.parent / ".env")`), not from `demo/`. Required keys: `PLAID_CLIENT_ID`, `PLAID_PRODUCTION_SECRET` / `PLAID_SANDBOX_SECRET`, `PLAID_ENV` (`production` or `sandbox`), and optionally `SNAPTRADE_CLIENT_ID` + `SNAPTRADE_CONSUMER_KEY`. Without SnapTrade credentials the server still runs — only Plaid data is collected. Tunables: `BALANCE_REFRESH_SECONDS` (default 3600), `TRANSACTION_LIMIT` (default 500, 0 = unlimited), `PORT` (default 8000).

There are no tests, no linter, no build step.

The `quickstart/` directory is a vendored copy of Plaid's official quickstart in many languages — it is reference material, not part of the running app. Don't edit it for app changes.

## Architecture

**Two-provider aggregator.** Plaid covers depository / credit / loan / investment accounts; SnapTrade covers brokerage accounts that Plaid can't link well (e.g. Fidelity stock plan, Robinhood). The server merges both into a single view. Every account gets a stable `account_key` (`plaid_account_key` / `snaptrade_account_key`) — a sha256 over identifying fields — so the same account survives re-syncs and can be referenced by hide/unhide. SnapTrade's identifiers shift across responses, so its key is built from many fields (id, brokerage authorization, number, institution, name, type) rather than a single id.

**Cache-first request model.** `start_balance_refresher()` spawns a daemon thread (`balance_refresher_loop`) that re-pulls balances → transactions → investments every `BALANCE_REFRESH_SECONDS`. The `/api/balances`, `/api/transactions`, `/api/investments` endpoints return the in-memory cache by default and only re-fetch when called with `?refresh=1`. On a refresh failure, `record_*_refresh_failure` keeps the previous cache and appends an error entry instead of clearing it. Caches are mirrored to disk (`balances_cache.json`, `transactions_cache.json`, `investments_cache.json`) so a restart is warm. All cache mutation goes through `BALANCE_CACHE_LOCK` / `TRANSACTIONS_CACHE_LOCK` / `INVESTMENTS_CACHE_LOCK` and uses `_json_clone` to avoid handing out shared references.

**Persistence files** (all live in `demo/`, all gitignored):

| File | Purpose |
| --- | --- |
| `tokens.json` | Plaid access tokens, one per linked institution. Source of truth for "what is linked." |
| `snaptrade_user.json` | The single SnapTrade user_id + user_secret. SnapTrade personal keys allow only one user; `ensure_snaptrade_user` recovers from "1012 / can only register one user" by deleting and re-registering. |
| `hidden_accounts.json` | `{"hidden": {account_key: {...meta, hidden_at}}}`. Hidden accounts are filtered out at collection time AND re-applied to the cached balance payload via `apply_hidden_accounts` so net worth stays consistent. |
| `net_worth_history.json` / `investment_history.json` | One snapshot per calendar day (today's entry is overwritten on re-fetch). Drives the chart on the frontend. |

**Investment normalization is the gnarly part.** `collect_investments` (and its mirror `normalized_investment_payload`, used when serving from cache) does several non-obvious transformations against raw Plaid/SnapTrade holdings:

- `is_unvested_stock_grant_holding` drops F5/FFIV stock-plan holdings over 200 shares & $50k — these appear in the brokerage feed but are unvested and would double-count net worth. Hardcoded to the user's specific employer; revisit before generalizing.
- `is_buying_power_holding` reclassifies cash-equivalents (USD, FDLXX, SPAXX, money market funds, "buying power") out of holdings and into the `buying_power.items` bucket so the allocation chart isn't dominated by cash.
- `is_stock_plan_account` + `reconciled_stock_plan_accounts` reconciles the *holding* value against the *account balance* for stock-plan accounts (Plaid often reports per-share value but a wrong total) — the account total wins, and only one holding per account survives.
- `accounts_from_investable_holdings` rebuilds the displayed account list from holdings rather than raw account balances, so cash-only and unvested-stock accounts disappear from the investment view.

If you change any of these heuristics, change them in **both** `collect_investments` (refresh path) and `normalized_investment_payload` (cached-read path) — they implement the same logic on different inputs.

**Frontend.** `index.html` is a single ~1450-line file with vanilla JS and three tabs (Overview, Investments, Transactions). It calls `/api/balances`, `/api/investments`, `/api/transactions` (with `?refresh=1` on user-triggered refresh), `/api/create_link_token` + `/api/set_access_token` for Plaid Link, `/api/create_update_link_token` for re-consenting an existing item, `/api/snaptrade/connect` for the SnapTrade redirect URL, `/api/accounts/hide` for hide, and `/api/reset` for wipe-everything. No framework, no bundler — edit the file in place.

## Conventions

- The `.env` file at the repo root contains real credentials and is gitignored. Don't commit it, don't echo its contents into anything that gets logged or pasted.
- Prefer adding env-var knobs (`os.getenv("X", default)`) over hardcoding numbers, following the existing `BALANCE_REFRESH_SECONDS` / `TRANSACTION_LIMIT` pattern.
- The categorization keys are fixed strings: `deposit`, `investment`, `debt`, `other`. `categorize()` maps Plaid `type` to these. Don't introduce new top-level categories without updating `empty_balance_payload`, `recompute_balance_payload`, and the frontend rendering together.
