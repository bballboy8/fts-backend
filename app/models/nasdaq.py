import logging
import os
import dotenv
import asyncpg
import asyncio

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

    # Base query
    query = "SELECT * FROM stock_data"

    # Adding filters if they are provided
    conditions = []
    if symbol:
        conditions.append(f"symbol = {symbol}")
    if start_datetime:
        conditions.append(f"date >= {start_datetime}")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    # Prepare values for the query
    values = []
    if symbol:
        values.append(symbol)
    if start_datetime:
        values.append(start_datetime)

    # Execute the query with the values
    records = await conn.fetch(query, *values)

    await conn.close()
    return records


# Example usage
if __name__ == "__main__":
    asyncio.run(fetch_all_data())
