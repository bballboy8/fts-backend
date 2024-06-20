import os
import aioboto3
import asyncio
import psycopg2
import logging
from tqdm import tqdm

# Database connection parameters
db_params = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": "5432",
}

# DynamoDB table name
table_name = "NASDAQ2"

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Function to insert a batch of records into the database
def insert_batch(batch):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        args_str = ",".join(
            cursor.mogrify(
                "(%s,%s,%s,%s,%s)",
                (
                    item.get("trackingID"),
                    item.get("date"),
                    item.get("msgType"),
                    item.get("symbol"),
                    item.get("price"),
                ),
            ).decode("utf-8")
            for item in batch
            if "trackingID" in item
            and "date" in item
            and "msgType" in item
            and "symbol" in item
            and "price" in item
        )
        if args_str:
            cursor.execute(
                "INSERT INTO stock_data (trackingID, date, msgType, symbol, price) VALUES "
                + args_str
            )
            conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting batch: {e}")


# Asynchronous function to fetch data from DynamoDB and insert into PostgreSQL in batches
async def fetch_and_insert():
    session = aioboto3.Session(
        aws_access_key_id=os.getenv("NASDQA_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("NASDQA_SECRET_ACCESS_KEY"),
        region_name=os.getenv("NASDQA_DEFAULT_REGION"),
    )
    async with session.resource("dynamodb") as dynamodb:
        table = await dynamodb.Table(table_name)
        scan_kwargs = {}
        done = False
        start_key = None
        total_items = 0

        # Determine total number of items for progress tracking
        count_response = await table.scan(Select="COUNT")
        total_items = count_response["Count"]

        with tqdm(total=total_items, desc="Processing records") as pbar:
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = await table.scan(**scan_kwargs)
                items = response.get("Items", [])
                if items:
                    insert_batch(items)
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
                pbar.update(len(items))


if __name__ == "__main__":
    # Connect to the database to drop the table if it exists and create a new one
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS stock_data")
    cursor.execute("""
        CREATE TABLE stock_data (
            trackingID VARCHAR(30),
            date TIMESTAMP,
            msgType VARCHAR(20),
            symbol VARCHAR(10),
            price FLOAT
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

    # Start the asynchronous fetching and inserting process
    asyncio.run(fetch_and_insert())

    logging.info("All batches have been inserted.")
