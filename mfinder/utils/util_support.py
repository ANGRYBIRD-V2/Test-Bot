import psycopg2
from psycopg2 import pool
from mfinder import ADMINS, DB_URL
import time

def is_admin(user_id):
    return user_id in ADMINS

def humanbytes(B):
    'Return the given bytes as a human-friendly KB, MB, GB, or TB string'
    B = float(B)
    KB = float(1024)
    MB = float(KB ** 2)  # 1,048,576
    GB = float(KB ** 3)  # 1,073,741,824
    TB = float(KB ** 4)  # 1,099,511,627,776

    if B < KB:
        return f'{B} {"Bytes" if 0 == B > 1 else "Byte"}'
    elif KB <= B < MB:
        return f'{B/KB:.2f} KB'
    elif MB <= B < GB:
        return f'{B/MB:.2f} MB'
    elif GB <= B < TB:
        return f'{B/GB:.2f} GB'
    elif TB <= B:
        return f'{B/TB:.2f} TB'

def get_db_size():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()
    query = "SELECT pg_database_size(current_database()) / (1024.0 * 1024.0)::numeric;"
    cursor.execute(query)
    database_size_mb = cursor.fetchone()[0]
    database_size_mb = float(
        database_size_mb) if database_size_mb is not None else 0.0
    db_size = round(database_size_mb, 2)
    cursor.close()
    conn.close()
    return db_size

# Connection Pooling
connection_pool = pool.SimpleConnectionPool(
    1, 10, DB_URL
)

def get_connection_with_retry(retries=5, delay=5):
    for attempt in range(retries):
        try:
            conn = connection_pool.getconn()
            if conn:
                return conn
        except psycopg2.OperationalError:
            if attempt < retries - 1:
                time.sleep(delay)
                continue
            else:
                raise
    raise Exception("Failed to get connection after multiple retries")

def release_connection(conn):
    if conn:
        connection_pool.putconn(conn)

def close_all_connections():
    connection_pool.closeall()
