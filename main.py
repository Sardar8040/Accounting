import argparse
import asyncio
import logging
import sys
from dotenv import load_dotenv
import os
import httpx
from bot.handlers import init_bot
from db.models import init_db



def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )


async def main(start_bot: bool = True) -> None:
    # Load environment variables
    load_dotenv()

    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    # Initialize database
    db_path = os.getenv("DB_PATH", "teleshop.db")
    logger.info("Initializing database: %s", db_path)
    await init_db(db_path)

    # If user requested no bot start (dry-run), exit after DB init
    if not start_bot:
        logger.info("Dry-run complete: DB initialized, exiting without starting bot.")
        return

    # Initialize bot
    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        logger.warning("TELEGRAM_TOKEN not set. Bot will not start. Use --start to force start if you have a token set in env.")
        return
    app = await init_bot()
    logger.info("Bot starting polling. Press Ctrl+C to stop.")
    await app.run_polling()


if __name__ == "__main__":
    # On Windows, prefer the SelectorEventLoopPolicy for compatibility
    if sys.platform.startswith("win"):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            # If policy isn't available on this Python build, continue without changing it
            pass

    parser = argparse.ArgumentParser(description="Teleshop bot runner")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="Initialize DB and exit without starting bot")
    args = parser.parse_args()

    try:
        asyncio.run(main(start_bot=not args.dry_run))
    except (KeyboardInterrupt, SystemExit):
        print("Shutting down")
