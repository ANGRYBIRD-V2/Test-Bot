import threading
from sqlalchemy import create_engine, Column, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.pool import QueuePool
from mfinder import DB_URL
import time
import sqlalchemy

BASE = declarative_base()

class BanList(BASE):
    __tablename__ = "banlist"
    user_id = Column(BigInteger, primary_key=True)

    def __init__(self, user_id):
        self.user_id = user_id

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

async def ban_user(user_id):
    with INSERTION_LOCK:
        try:
            usr = SESSION.query(BanList).filter_by(user_id=user_id).one()
        except NoResultFound:
            usr = BanList(user_id=user_id)
            SESSION.add(usr)
            SESSION.commit()
            return True
        except sqlalchemy.exc.OperationalError as e:
            SESSION.rollback()
            raise e
        finally:
            SESSION.close()

async def is_banned(user_id, retries=3):
    with INSERTION_LOCK:
        for attempt in range(retries):
            try:
                usr = SESSION.query(BanList).filter_by(user_id=user_id).one()
                return usr.user_id
            except NoResultFound:
                return False
            except sqlalchemy.exc.OperationalError as e:
                if 'SSL connection has been closed unexpectedly' in str(e):
                    if attempt < retries - 1:
                        time.sleep(2)  # wait before retrying
                        continue
                raise e
            except sqlalchemy.exc.PendingRollbackError:
                SESSION.rollback()
                if attempt < retries - 1:
                    time.sleep(2)  # wait before retrying
                    continue
                raise
            finally:
                SESSION.close()

async def unban_user(user_id):
    with INSERTION_LOCK:
        try:
            usr = SESSION.query(BanList).filter_by(user_id=user_id).one()
            SESSION.delete(usr)
            SESSION.commit()
            return True
        except NoResultFound:
            return False
        except sqlalchemy.exc.OperationalError as e:
            SESSION.rollback()
            raise e
        finally:
            SESSION.close()
