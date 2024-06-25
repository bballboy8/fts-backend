import os
import dotenv
import asyncpg
import asyncio
from datetime import datetime

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


async def fetch_all_data(symbol=None, start_datetime=None):
    conn = await asyncpg.connect(
        database=db_params["dbname"],
        user=db_params["user"],
        password=db_params["password"],
        host=db_params["host"],
        port=db_params["port"],
    )

    # Convert start_datetime to a datetime object if provided
    if start_datetime:
        start_datetime = datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")

    # Base query
    query = "SELECT * FROM stock_data where msgType = 'T'"
    conditions = []
    values = []

    # Adding filters if they are provided
    if symbol:
        conditions.append(" and symbol = $1")
        values.append(symbol)
    if start_datetime:
        conditions.append(f" and date >= ${len(values) + 1}::timestamp")
        values.append(start_datetime)

    if conditions:
        query += " ".join(conditions)

    logger.info(f"Executing query: {query}")
    logger.info(f"With values: {values}")

    try:
        # Execute the query with the values
        records = await conn.fetch(query, *values)
    except Exception as e:
        logger.error(f"Error executing query: {e}", exc_info=True)
        raise
    finally:
        await conn.close()

    return records


# Example usage
if __name__ == "__main__":
    asyncio.run(fetch_all_data(symbol=None, start_datetime="2023-06-19 14:30:00"))
