import threading
import time
from sqlalchemy import create_engine, Column, TEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError, PendingRollbackError
from mfinder import DB_URL

BASE = declarative_base()

class Filters(BASE):
    __tablename__ = "filters"
    filters = Column(TEXT, primary_key=True)
    message = Column(TEXT)

    def __init__(self, filters, message):
        self.filters = filters
        self.message = message

def start() -> scoped_session:
    engine = create_engine(
        DB_URL,
        connect_args={"sslmode": "require"},
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=1800  # Recycle connections every 1800 seconds (30 minutes)
    )
    BASE.metadata.bind = engine
    BASE.metadata.create_all(engine)
    return scoped_session(sessionmaker(bind=engine, autoflush=False))

SESSION = start()
INSERTION_LOCK = threading.RLock()

def reconnect_session():
    global SESSION
    SESSION.close()
    SESSION = start()

async def add_filter(filters, message):
    with INSERTION_LOCK:
        try:
            fltr = SESSION.query(Filters).filter(Filters.filters.ilike(filters)).one()
        except NoResultFound:
            fltr = Filters(filters=filters, message=message)
            SESSION.add(fltr)
            SESSION.commit()
            return True

async def is_filter(filters):
    retries = 3
    while retries > 0:
        with INSERTION_LOCK:
            try:
                fltr = SESSION.query(Filters).filter(Filters.filters.ilike(filters)).one()
                return fltr
            except NoResultFound:
                return False
            except OperationalError as e:
                if 'SSL connection has been closed unexpectedly' in str(e):
                    if retries > 1:
                        time.sleep(2)  # wait before retrying
                        reconnect_session()
                        retries -= 1
                        continue
                raise e
            except PendingRollbackError:
                SESSION.rollback()
                if retries > 1:
                    time.sleep(2)  # wait before retrying
                    retries -= 1
                    continue
                raise
            finally:
                SESSION.close()

async def rem_filter(filters):
    with INSERTION_LOCK:
        try:
            fltr = SESSION.query(Filters).filter(Filters.filters.ilike(filters)).one()
            SESSION.delete(fltr)
            SESSION.commit()
            return True
        except NoResultFound:
            return False

async def list_filters():
    retries = 3
    while retries > 0:
        try:
            fltrs = SESSION.query(Filters.filters).all()
            return [fltr[0] for fltr in fltrs]
        except NoResultFound:
            return False
        except OperationalError as e:
            if 'SSL connection has been closed unexpectedly' in str(e):
                if retries > 1:
                    time.sleep(2)  # wait before retrying
                    reconnect_session()
                    retries -= 1
                    continue
            raise e
        except PendingRollbackError:
            SESSION.rollback()
            if retries > 1:
                time.sleep(2)  # wait before retrying
                retries -= 1
                continue
            raise
        finally:
            SESSION.close()
