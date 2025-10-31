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


def main(start_bot: bool = True) -> None:
    # Load environment variables
    load_dotenv()

    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    # Initialize database (run the synchronous initializer which calls async DB init)
    db_path = os.getenv("DB_PATH", "teleshop.db")
    logger.info("Initializing database: %s", db_path)
    asyncio.run(init_db(db_path))

    # If user requested no bot start (dry-run), exit after DB init
    if not start_bot:
        logger.info("Dry-run complete: DB initialized, exiting without starting bot.")
        return

    # Initialize bot (init_bot is async so run it via asyncio.run)
    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        logger.warning("TELEGRAM_TOKEN not set. Bot will not start. Set TELEGRAM_TOKEN in .env or environment.")
        return

    # Build the Application instance using its async initializer but run the polling in the
    # synchronous context so Application.run_polling() can manage the loop lifecycle itself.
    app = asyncio.run(init_bot())
    logger.info("Bot starting polling. Press Ctrl+C to stop.")
    # Ensure there is an event loop set in this thread; some Python versions
    # raise if no loop is present when libraries call asyncio.get_event_loop().
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    except Exception:
        # If we can't set a new loop, continue and let the library handle it.
        pass
    # run_polling is a blocking call that handles loop setup/teardown internally.
    app.run_polling()


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
        main(start_bot=not args.dry_run)
    except (KeyboardInterrupt, SystemExit):
        print("Shutting down")
