import asyncio
import logging
from dotenv import load_dotenv
import os
import nest_asyncio  # to fix VS Code debug event loop issue
import httpx
from bot.handlers import init_bot
from db.models import init_db



def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )


async def main() -> None:
    # Load environment variables
    load_dotenv()

    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    # Initialize database
    db_path = os.getenv("DB_PATH", "teleshop.db")
    logger.info("Initializing database: %s", db_path)
    await init_db(db_path)

    # Initialize bot
    app = await init_bot()
    logger.info("Bot starting polling. Press Ctrl+C to stop.")
    await app.run_polling()


if __name__ == "__main__":
    # Fix for VS Code / interactive environments
    nest_asyncio.apply()

    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Shutting down")
