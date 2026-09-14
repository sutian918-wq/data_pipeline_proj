"""
PostgreSQL writer using a connection pool.

Uses psycopg2 with SimpleConnectionPool for thread-safe access.
"""
from src.writers import BaseWriter
from psycopg2 import pool
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)

class PostgresWriter(BaseWriter):
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str, min_conn: int = 1, max_conn: int = 10):
        """
        Create a connection pool and ensure the table exists.

        Args:
            host, port, dbname, user, password: PostgreSQL credentials.
            min_conn: Minimum connections in the pool.
            max_conn: Maximum connections (should be >= worker count).
        """
        self.pool = pool.ThreadedConnectionPool(
            min_conn, max_conn, 
            host=host, port=port, dbname=dbname, 
            user=user, password=password
        )
        self._create_table()
        logger.info(f"PostgresWriter ready (pool size: {min_conn} - {max_conn})")

    def _create_table(self) -> None:
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                # UNIQUE (partition_id, offset_id) ensures idempotent inserts
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        id SERIAL PRIMARY KEY,
                        ticker VARCHAR(10) NOT NULL,
                        price NUMERIC(10,2) NOT NULL,
                        volume INT,
                        event_time DOUBLE PRECISION NOT NULL,
                        datetime TIMESTAMPZ,
                        created_at TIMESTAMPZ DEFAULT CURRENT_TIMESTAMP,
                        partition_id INT,
                        offset_id BIGINT, 
                        UNIQUE (partition_id, offset_id)
                    );
                """)
            conn.commit()
        finally:
            self.pool.putconn(conn)

    def insert(self, data: dict) -> None:
        """Insert one record."""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO events
                        (ticker, price, volume, event_time, datetime, partition_id, offset_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (partition_id, offset_id) DO NOTHING
                """, (
                    data["ticker"], 
                    data["price"],
                    data.get("volume"), 
                    data["timestamp"], 
                    data.get("datetime"),
                    data.get("partition_id"),
                    data.get("offset_id")
                ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Insert failed: {e}")
            raise
        finally:
            self.pool.putconn(conn)

    def insert_batch(self, data_list: list[dict]):
        """Insert many records in a single query."""
        if not data_list:
            return
        
        conn = self.pool.getconn()

        try:
            with conn.cursor() as cur:
                values = [
                    (
                        data["ticker"], 
                        data["price"],
                        data.get("volume"), 
                        data["timestamp"], 
                        data.get("datetime"),
                        data.get("partition_id"),
                        data.get("offset_id")
                    )
                    for data in data_list
                ]
                execute_values(
                    cur, 
                    """
                        INSERT INTO events
                            (ticker, price, volume, timestamp, datetime, partition_id, offset_id)
                        VALUES %s
                        ON CONFLICT (partition_id, offset_id) DO NOTHING
                    """,
                    values
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Batch insert failed: {e}")
            raise
        finally:
            self.pool.putconn(conn)

    def close(self) -> None:
        """Close all connections in the pool."""
        self.pool.closeall()
        logger.info("PostgresWriter closed")


        




