import logging
from typing import List, Dict, Any
import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)

def get_postgres_pool(config: Dict[str, Any]) -> Any:
    """Create and return a PostgreSQL connection pool."""
    logger.info("Creating PostgreSQL connection pool")
    try:
        pg_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20,
            host=config['host_port'].split(':')[0],
            port=config['host_port'].split(':')[1],
            database=config['database'],
            user=config['username'],
            password=config['password']
        )
        logger.info("Successfully created PostgreSQL connection pool")
        return pg_pool
    except Exception as e:
        logger.error(f"Failed to create PostgreSQL connection pool: {e}")
        raise

def check_postgres_tables_exist(pg_pool: Any, pg_config: Dict[str, Any]) -> None:
    """Check if required tables exist in PostgreSQL."""
    with pg_pool.getconn() as conn:
        with conn.cursor() as cur:
            for table in ['ais0112', 'ais0113']:
                cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{pg_config['username']}'
                    AND table_name = '{table}'
                );
                """)
                exists = cur.fetchone()[0]
                if not exists:
                    logger.error(f"Error: The table '{table}' does not exist in the PostgreSQL database.")
                else:
                    logger.info(f"Table '{table}' exists in PostgreSQL")
        pg_pool.putconn(conn)

async def transfer_to_postgres(pg_pool: Any, table_name: str, data: List[Dict[str, Any]], batch_size: int) -> None:
    """Transfer data to PostgreSQL table."""
    logger.info(f"Starting transfer to PostgreSQL table {table_name}")
    
    conn = pg_pool.getconn()
    try:
        with conn.cursor() as cur:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                columns = ', '.join(batch[0].keys())
                placeholders = ', '.join(['%s'] * len(batch[0]))
                insert_query = f"INSERT INTO dlusr.{table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                
                cur.executemany(insert_query, [tuple(item.values()) for item in batch])
                
            conn.commit()
            logger.info(f"Committed changes to PostgreSQL for {table_name}")
    except Exception as e:
        logger.error(f"Error during transfer to PostgreSQL table {table_name}: {e}")
        conn.rollback()
    finally:
        pg_pool.putconn(conn)

    logger.info(f"Finished transfer to PostgreSQL table {table_name}")
