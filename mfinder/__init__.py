import os
import re
import logging
import logging.config
import asyncio
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool, NullPool

load_dotenv()

id_pattern = re.compile(r"^.\d+$")

# vars
APP_ID = os.environ.get("APP_ID", "")
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DB_URL = os.environ.get("DB_URL", "")
OWNER_ID = int(os.environ.get("OWNER_ID", ""))
ADMINS = [
    int(user) if id_pattern.search(user) else user
    for user in os.environ.get("ADMINS", "").split()
] + [OWNER_ID]
DB_CHANNELS = [
    int(ch) if id_pattern.search(ch) else ch
    for ch in os.environ.get("DB_CHANNELS", "").split()
]

try:
    import const
except Exception:
    import sample_const as const

START_MSG = const.START_MSG
START_KB = const.START_KB
HELP_MSG = const.HELP_MSG
HELP_KB = const.HELP_KB

# logging Conf
logging.config.fileConfig(fname="config.ini", disable_existing_loggers=False)
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# Database connection
def start():
    engine = create_engine(
        DB_URL,
        connect_args={"sslmode": "require"},
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=1800  # Recycle connections every 1800 seconds (30 minutes)
    )
    return scoped_session(sessionmaker(bind=engine, autoflush=False))

SESSION = start()

# Keep-alive task
async def keep_alive():
    while True:
        try:
            # Execute a simple query to keep the connection alive
            with SESSION() as session:
                session.execute("SELECT 1")
            await asyncio.sleep(180)  # Sleep for 3 minutes
        except Exception as e:
            LOGGER.warning(f"Keep-alive error: {e}")
            await asyncio.sleep(10)  # Wait before retrying

# Main function
async def main():
    # Start the keep-alive task
    asyncio.create_task(keep_alive())
    # Your existing bot initialization and running code

if __name__ == "__main__":
    asyncio.run(main())
