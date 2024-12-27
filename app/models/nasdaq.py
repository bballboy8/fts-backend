import os
import time
from typing import Optional
import dotenv
import asyncio
from datetime import datetime
import pytz
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
    conn,
    symbol: Optional[str],
    start_datetime: Optional[datetime],
    end_datetime: Optional[datetime] = None,
):
    start_time = time.time()

    # Calculate the current time in EST timezone
    est_tz = pytz.timezone("US/Eastern")
    current_est_time = datetime.now(est_tz)
    formatted_est_time = current_est_time.strftime("%Y-%m-%dT%H:%M:%S")
    est_time = datetime.strptime(formatted_est_time, "%Y-%m-%dT%H:%M:%S")

    connection_acquire_time = time.time()
    logger.info("Acquired connection from pool")

    # Base query
    query = "SELECT date, symbol, size, price, msgtype FROM stock_data_partitioned WHERE msgType in ('T', 'H')"
    conditions = []
    values = []

    if symbol:
        conditions.append(f"symbol = ${len(values) + 1}")
        values.append(symbol)
    if start_datetime:
        conditions.append(f"date >= ${len(values) + 1}::timestamp")
        values.append(start_datetime)
    if end_datetime:
        conditions.append(f"date <= ${len(values) + 1}::timestamp")
        values.append(end_datetime)

    # Add condition to check that the date is <= current EST time
    else:
        conditions.append(f"date <= ${len(values) + 1}::timestamp")
        values.append(est_time)

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
        logger.info(f"Total fetch_all_data time: {end_time - start_time:.2f} seconds")


async def fetch_all_tickers(conn):
    # Base query
    query = "select * from mv_stock_data_symbol_count"
    logger.info(f"Executing query: {query}")

    try:
        # Execute the query with the values
        records = await conn.fetch(query)
    except Exception as e:
        logger.error(f"Error executing query: {e}", exc_info=True)
        raise
    return records


async def is_ticker_valid(ticker, conn):
    # Base query
    query = (
        f"select symbol from stock_data_partitioned where symbol = '{ticker}' limit 1"
    )

    logger.info(f"Executing query: {query}")

    try:
        # Execute the query with the values
        records = await conn.fetch(query)
    except Exception as e:
        logger.error(f"Error executing query: {e}", exc_info=True)
        raise

    return True if records else False


# Example usage
if __name__ == "__main__":
    asyncio.run(fetch_all_data(symbol=None, start_datetime="2023-06-19 14:30:00"))
