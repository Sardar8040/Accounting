## Quick context

This repository is a small async Telegram bot + SQLite backend for Teleshop reporting.
Key entrypoints:
- `main.py` — app startup: loads `.env`, initializes DB (`db.init_db`) and starts the bot (`bot.handlers.init_bot`).
- `bot/handlers.py` — all Telegram Command/Message handlers and `init_bot()` where handlers are registered.
- `db/models.py` — synchronous SQLite helpers (executed via `asyncio.to_thread`) and DB schema/migrations.
- `utils/excel_utils.py` — Excel parsing helpers: `parse_sales_excel`, `parse_pickup_excel`, `extract_daily_regs`.

## Big-picture architecture

- Telegram bot (async) calls into synchronous DB helpers. Most `db` functions are blocking and intentionally wrapped with `asyncio.to_thread` in their public async wrappers.
- Upload flow (bot -> utils -> db): handler `handle_document` saves upload to `uploads/<username>/`, calls `parse_sales_excel` (in a thread), then calls `db.insert_sales_and_update_inventory` which inserts rows and adjusts inventory atomically.
- Important invariant: "last-upload-wins" for daily uploads — handlers delete previous sales/daily_regs for staff/date before inserting new ones.
- sim_batches table holds SIM metadata (gsm_number is UNIQUE) and is used by pickup/import and transfer flows.

## Critical developer workflows

- Run locally (PowerShell):
  python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
  Copy `.env.example` → `.env` and set `TELEGRAM_TOKEN` (required) and `DB_PATH` (optional).
  Run bot: `python main.py` (main handles nest_asyncio for debug/VSCode).
- Tests: tests use pytest. `tests/conftest.py` adds project root to `sys.path`. Run `pytest` after activating the venv.

## Project-specific conventions & gotchas for AI edits

- DB helpers must remain synchronous functions that operate on sqlite3.Connection objects. Expose async wrappers that call the sync helpers via `asyncio.to_thread`. Pattern: keep heavy logic in sync `_fn()` and return via `await asyncio.to_thread(_fn)`.
- When changing sales/inventory logic prefer editing `insert_sales_and_update_inventory` in `db/models.py` because it centralizes atomic insert + journal behavior. It returns a summary dict: {skipped, duplicates_skipped, insufficient_skipped, inserted}.
- Excel parsing contract: `parse_sales_excel(file_bytes, report_date, employee_name)` → (entries, errors, daily_regs). If `errors` non-empty parsing failed. `entries` are dicts with keys like `item_code`, `number`, `recharge_amount`, `credit_50`, `credit_100`, `gsm_number`, `notes`.
- Pickup import: `parse_pickup_excel(bytes) -> list[rows]` with keys `carton_no, box_no, gsm_number, iccid, type` and `db.insert_pickup_list` handles insertion and duplicate counts.
- Preserve 'last-upload-wins' semantics in handlers: handlers delete previous entries for the staff/date before calling insertion helpers.
- Inventory columns map via `_map_item_to_column(item)`; prefer using existing item aliases (`sim`, `swap`, `credit_50`, `credit_100`).

## Integration points & env flags

- Environment variables:
  - `TELEGRAM_TOKEN` (required)
  - `DB_PATH` (defaults to `teleshop.db`)
  - `ADMIN_IDS` (comma-separated user ids for super-admins)
  - `ADMIN_NOTIFY_CHAT_ID` (single chat id to send admin notifications)
- File storage:
  - Uploads go to `uploads/<username>/` and filenames like `<username>_<YYYY-MM-DD>.xlsx`.
  - Reports are written to `reports/`.

## Adding handlers / common edits (example)

- To add a command: in `bot/handlers.py` write an async handler function (follow parameter types `update: Update, context: ContextTypes.DEFAULT_TYPE`), then register it in `init_bot()` with the appropriate `CommandHandler` or `MessageHandler`. If the command requires admin-checking, wrap with `_require_admin(...)`.
- When the handler calls DB helpers, prefer using the async wrappers in `db/models.py` (do not import `sqlite3` in handlers).

## Tests & editing guidance

- Tests import project root using `tests/conftest.py`; avoid relative imports that rely on working directory changes.
- When adding unit tests for excel parsing, test `utils/excel_utils.parse_sales_excel` with small in-memory bytes. For DB-related tests, create a temporary `DB_PATH` or use SQLite `:memory:` and call `init_db()` first.

## Files to inspect for examples

- `bot/handlers.py` — upload flow, handler patterns, `init_bot()` registration
- `db/models.py` — DB schema, migrations, insert_sales_and_update_inventory, journaling logic
- `utils/excel_utils.py` — parsing rules, column aliasing, examples of tolerated Excel shapes
- `tests/` — existing pytest cases demonstrating expected behavior

If anything here is unclear or you want more detail about a specific component (e.g., exact DB columns used for journaling or the shape of `sales` rows), tell me which part and I will expand or adjust the instructions.