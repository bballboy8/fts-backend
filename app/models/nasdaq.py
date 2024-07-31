import os
import time
from typing import Optional
import dotenv
import asyncpg
import asyncio
from datetime import datetime

from fastapi import HTTPException

from app.application_logger import get_logger

dotenv.load_dotenv()

logger = get_logger(__name__)

db_params = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": "5432",
}


async def fetch_all_data(
    pool,
    symbol: Optional[str],
    start_datetime: Optional[datetime],
):
    start_time = time.time()
    async with pool.acquire() as conn:
        connection_acquire_time = time.time()
        logger.info("Acquired connection from pool")

        # Base query
        query = "SELECT date, symbol, size, price FROM stock_data_partitioned WHERE msgType = 'T'"
        conditions = []
        values = []

        if symbol:
            conditions.append(f"symbol = ${len(values) + 1}")
            values.append(symbol)
        if start_datetime:
            conditions.append(f"date >= ${len(values) + 1}::timestamp")
            values.append(start_datetime)

        if conditions:
            query += " AND " + " AND ".join(conditions)

        query_prepare_time = time.time()
        logger.info(
            f"Query prepared in {query_prepare_time - connection_acquire_time:.2f} seconds"
        )
        logger.info(f"Executing query: {query}")
        logger.info(f"With values: {values}")

        # Construct the query for logging with actual values
        logged_query = query
        for i, val in enumerate(values, 1):
            if isinstance(val, str):
                val = f"'{val}'"
            elif isinstance(val, datetime):
                val = f"'{val.isoformat()}'"
            logged_query = logged_query.replace(f"${i}", str(val), 1)

        logger.info(f"Executing query: {logged_query}")

        try:
            query_start_time = time.time()
            records = await conn.fetch(query, *values)
            query_end_time = time.time()
            logger.info(
                f"Query executed in {query_end_time - query_start_time:.2f} seconds"
            )
            logger.info(f"Fetched {len(records)} records")
            return records
        except Exception as e:
            logger.error(f"Error executing query: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            end_time = time.time()
            logger.info(
                f"Total fetch_all_data time: {end_time - start_time:.2f} seconds"
            )


async def fetch_all_tickers():
    conn = await asyncpg.connect(
        database=db_params["dbname"],
        user=db_params["user"],
        password=db_params["password"],
        host=db_params["host"],
        port=db_params["port"],
    )

    # Base query
    query = "select * from mv_stock_data_symbol_count"

    logger.info(f"Executing query: {query}")

    try:
        # Execute the query with the values
        records = await conn.fetch(query)
    except Exception as e:
        logger.error(f"Error executing query: {e}", exc_info=True)
        raise
    finally:
        await conn.close()

    return records


# Example usage
if __name__ == "__main__":
    asyncio.run(fetch_all_data(symbol=None, start_datetime="2023-06-19 14:30:00"))
