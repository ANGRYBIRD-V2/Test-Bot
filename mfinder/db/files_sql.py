import threading
import time
from sqlalchemy import create_engine, or_, func, and_
from sqlalchemy import Column, TEXT, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError, PendingRollbackError, NoResultFound
from mfinder import DB_URL, LOGGER
from mfinder.utils.helpers import unpack_new_file_id
import asyncio

BASE = declarative_base()

class Files(BASE):
    __tablename__ = "files"
    file_name = Column(TEXT, primary_key=True)
    file_id = Column(TEXT)
    file_ref = Column(TEXT)
    file_size = Column(Numeric)
    file_type = Column(TEXT)
    mime_type = Column(TEXT)
    caption = Column(TEXT)

    def __init__(self, file_name, file_id, file_ref, file_size, file_type, mime_type, caption):
        self.file_name = file_name
        self.file_id = file_id
        self.file_ref = file_ref
        self.file_size = file_size
        self.file_type = file_type
        self.mime_type = mime_type
        self.caption = caption

def start() -> scoped_session:
    engine = create_engine(
        DB_URL,
        connect_args={"sslmode": "require"},
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=1800
    )
    BASE.metadata.bind = engine
    BASE.metadata.create_all(engine)
    return scoped_session(sessionmaker(bind=engine, autoflush=False))

SESSION = start()
INSERTION_LOCK = threading.RLock()

def reconnect_session(max_retries=5, delay=5):
    """Attempt to reconnect to the database a specified number of times with a delay."""
    for attempt in range(max_retries):
        try:
            global SESSION
            SESSION = start()
            return SESSION
        except OperationalError as e:
            LOGGER.warning(f"Database connection failed: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Failed to reconnect to the database after multiple attempts")

async def save_file(media):
    """Save a media file to the database."""
    file_id, file_ref = unpack_new_file_id(media.file_id)
    with INSERTION_LOCK:
        try:
            file = SESSION.query(Files).filter_by(file_id=file_id).one()
            LOGGER.warning("%s is already saved in the database", media.file_name)
        except NoResultFound:
            try:
                file = SESSION.query(Files).filter_by(file_name=media.file_name, file_size=media.file_size).one()
                LOGGER.warning("%s with size %s is already saved in the database", media.file_name, media.file_size)
            except NoResultFound:
                file = Files(
                    file_name=media.caption if media.caption else media.file_name,
                    file_id=file_id,
                    file_ref=file_ref,
                    file_size=media.file_size,
                    file_type=media.file_type,
                    mime_type=media.mime_type,
                    caption=media.caption if media.caption else media.file_name,
                )
                LOGGER.info("%s is saved in the database", media.file_name)
                SESSION.add(file)
                SESSION.commit()
                return True
            except Exception as e:
                LOGGER.warning("Error occurred while saving file in the database: %s", str(e))
                SESSION.rollback()
                return False
        except Exception as e:
            LOGGER.warning("Error occurred while saving file in the database: %s", str(e))
            SESSION.rollback()
            return False
        finally:
            SESSION.close()

async def get_filter_results(query, page=1, per_page=10):
    """Get filtered results from the database."""
    retries = 3
    while retries > 0:
        try:
            with INSERTION_LOCK:
                offset = (page - 1) * per_page
                search = query.split()
                conditions = []
                for word in search:
                    conditions.append(
                        or_(
                            Files.file_name.ilike(f"%{word}%"),
                            Files.caption.ilike(f"%{word}%"),
                        )
                    )
                combined_condition = and_(*conditions)
                files_query = (
                    SESSION.query(Files)
                    .filter(combined_condition)
                    .order_by(Files.file_name)
                )
                total_count = files_query.count()
                files = files_query.offset(offset).limit(per_page).all()
                return files, total_count
        except PendingRollbackError:
            SESSION.rollback()
            retries -= 1
            continue
        except OperationalError as e:
            LOGGER.warning(f"OperationalError: {e}. Retrying...")
            reconnect_session()
            retries -= 1
        except Exception as e:
            LOGGER.warning(f"Error occurred while retrieving filter results: {e}")
            return [], 0
        finally:
            try:
                SESSION.close()
            except Exception as close_error:
                LOGGER.error(f"Error closing session: {close_error}")
    return [], 0

async def get_precise_filter_results(query, page=1, per_page=10):
    """Get precise filtered results from the database."""
    retries = 3
    while retries > 0:
        try:
            with INSERTION_LOCK:
                offset = (page - 1) * per_page
                search = query.split()
                conditions = []
                for word in search:
                    conditions.append(
                        or_(
                            func.concat(" ", Files.file_name, " ").ilike(f"% {word} %"),
                            func.concat(" ", Files.caption, " ").ilike(f"% {word} %"),
                        )
                    )
                combined_condition = and_(*conditions)
                files_query = (
                    SESSION.query(Files)
                    .filter(combined_condition)
                    .order_by(Files.file_name)
                )
                total_count = files_query.count()
                files = files_query.offset(offset).limit(per_page).all()
                return files, total_count
        except PendingRollbackError:
            SESSION.rollback()
            retries -= 1
            continue
        except OperationalError as e:
            LOGGER.warning(f"OperationalError: {e}. Retrying...")
            reconnect_session()
            retries -= 1
        except Exception as e:
            LOGGER.warning(f"Error occurred while retrieving filter results: {e}")
            return [], 0
        finally:
            try:
                SESSION.close()
            except Exception as close_error:
                LOGGER.error(f"Error closing session: {close_error}")
    return [], 0

async def get_file_details(file_id):
    """Get file details based on file_id and generate a download link."""
    retries = 3
    while retries > 0:
        try:
            with INSERTION_LOCK:
                file_details = SESSION.query(Files).filter_by(file_id=file_id).all()
                if file_details:
                    # Generate the direct download link
                    download_link = f"https://yourserver.com/download/{file_id}"
                    return file_details, download_link
                return None, None
        except PendingRollbackError:
            SESSION.rollback()
            retries -= 1
            continue
        except OperationalError as e:
            LOGGER.warning(f"OperationalError: {e}. Retrying...")
            reconnect_session()
            retries -= 1
        except Exception as e:
            LOGGER.warning(f"Error occurred while retrieving file details: {e}")
            return None, None
        finally:
            try:
                SESSION.close()
            except Exception as close_error:
                LOGGER.error(f"Error closing session: {close_error}")
    return None, None

async def delete_file(media):
    """Delete a file record from the database."""
    file_id, file_ref = unpack_new_file_id(media.file_id)
    retries = 3
    while retries > 0:
        try:
            with INSERTION_LOCK:
                file = SESSION.query(Files).filter_by(file_id=file_id).first()
                if file:
                    SESSION.delete(file)
                    SESSION.commit()
                    return True
                return "Not Found"
                LOGGER.warning("File to delete not found: %s", str(file_id))
        except PendingRollbackError:
            SESSION.rollback()
            retries -= 1
            continue
        except OperationalError as e:
            LOGGER.warning(f"OperationalError: {e}. Retrying...")
            reconnect_session()
            retries -= 1
        except Exception as e:
            LOGGER.warning(f"Error occurred while deleting file: {e}")
            SESSION.rollback()
            return False
        finally:
            try:
                SESSION.close()
            except Exception as close_error:
                LOGGER.error(f"Error closing session: {close_error}")
    return False

async def count_files():
    """Count the total number of files in the database."""
    retries = 3
    while retries > 0:
        try:
            with INSERTION_LOCK:
                total_count = SESSION.query(Files).count()
                return total_count
        except PendingRollbackError:
            SESSION.rollback()
            retries -= 1
            continue
        except OperationalError as e:
            LOGGER.warning(f"OperationalError: {e}. Retrying...")
            reconnect_session()
            retries -= 1
        except Exception as e:
            LOGGER.warning(f"Error occurred while counting files: {e}")
            return 0
        finally:
            try:
                SESSION.close()
            except Exception as close_error:
                LOGGER.error(f"Error closing session: {close_error}")
    return 0

async def keep_alive():
    """Keep the database connection alive."""
    while True:
        try:
            with SESSION() as session:
                session.execute("SELECT 1")
            await asyncio.sleep(180)
        except Exception as e:
            LOGGER.warning(f"Keep-alive error: {e}")
            await asyncio.sleep(10)

async def main():
    asyncio.create_task(keep_alive())

if __name__ == "__main__":
    asyncio.run(main())
