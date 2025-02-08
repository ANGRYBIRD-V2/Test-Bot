import threading
from sqlalchemy import create_engine, Column, TEXT, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.pool import StaticPool
from mfinder import DB_URL

BASE = declarative_base()

class Broadcast(BASE):
    __tablename__ = "broadcast"
    user_id = Column(BigInteger, primary_key=True)
    user_name = Column(TEXT)

    def __init__(self, user_id, user_name):
        self.user_id = user_id
        self.user_name = user_name

def start() -> scoped_session:
    engine = create_engine(
        DB_URL,
        connect_args={"sslmode": "require"},
        client_encoding="utf8",
        poolclass=StaticPool,
        pool_pre_ping=True  # Added to handle disconnections
    )
    BASE.metadata.bind = engine
    BASE.metadata.create_all(engine)
    return scoped_session(sessionmaker(bind=engine, autoflush=False))

SESSION = start()
INSERTION_LOCK = threading.RLock()

async def add_user(user_id, user_name):
    with INSERTION_LOCK:
        try:
            usr = SESSION.query(Broadcast).filter_by(user_id=user_id).one()
        except NoResultFound:
            usr = Broadcast(user_id=user_id, user_name=user_name)
            SESSION.add(usr)
            SESSION.commit()
        except Exception as e:
            SESSION.rollback()
            raise e
        finally:
            SESSION.close()

async def is_user(user_id):
    with INSERTION_LOCK:
        try:
            usr = SESSION.query(Broadcast).filter_by(user_id=user_id).one()
            return usr.user_id
        except NoResultFound:
            return False
        except Exception as e:
            SESSION.rollback()
            raise e
        finally:
            SESSION.close()

async def query_msg():
    try:
        query = SESSION.query(Broadcast.user_id).order_by(Broadcast.user_id)
        return query.all()
    except Exception as e:
        SESSION.rollback()
        raise e
    finally:
        SESSION.close()

async def del_user(user_id):
    with INSERTION_LOCK:
        try:
            usr = SESSION.query(Broadcast).filter_by(user_id=user_id).one()
            SESSION.delete(usr)
            SESSION.commit()
        except NoResultFound:
            pass
        except Exception as e:
            SESSION.rollback()
            raise e
        finally:
            SESSION.close()
