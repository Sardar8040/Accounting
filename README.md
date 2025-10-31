Teleshop Auto Reporting & Stock Management System
=================================================

This project implements a Telegram bot that ingests daily sales Excel files from staff, parses them, stores sales and inventory in SQLite, and generates daily recharge reports.

Setup (Windows PowerShell)

1. Create a virtual environment and install dependencies:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
```

2. Copy `.env.example` to `.env` and set `TELEGRAM_TOKEN` and optional `DB_PATH`.

3. Run the bot:

```powershell
python main.py
```

Usage

- /start - welcome message
- /help - available commands
- /summary - show current stock for the user
- Upload an Excel file as a document (not as photo). The bot will parse and reply with sales summary and remaining stock.

Project Layout

- `main.py` - app entrypoint
- `bot/handlers.py` - telegram handlers
- `db/models.py` - sqlite schema and helpers
- `utils/excel_utils.py` - Excel parsing

Notes

- This is a minimal, modular implementation intended to be extended. It uses asyncio and python-telegram-bot v21+ patterns.
