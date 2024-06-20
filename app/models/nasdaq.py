import asyncio
import logging
from datetime import datetime, time
import os
import pytz
import aioboto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import dotenv

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_CONCURRENT_QUERIES = 10  # Increase the number of concurrent queries
QUERY_LIMIT = 10000  # Increase the query limit to fetch more items per batch


def calculateNanoSec(hour, min, sec, milisec):
    return (hour * 3600 + min * 60 + sec) * 1000 * 1000000 + milisec * 1000000


async def get_nasdaq_data(date=None, symbol="AAPL"):
    start_time = datetime.now()
    logger.info(f"Start time: {start_time}")

    session = aioboto3.Session(
        aws_access_key_id=os.getenv("NASDQA_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("NASDQA_SECRET_ACCESS_KEY"),
        region_name=os.getenv("NASDQA_DEFAULT_REGION"),
    )
    async with session.resource("dynamodb") as dynamodb:
        try:
            nasdaq_table = await dynamodb.Table("NASDAQ2")
            await nasdaq_table.load()
            logger.info(f"Loaded table at {datetime.now()}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                logger.error("The table NASDAQ2 or index fetch_index does not exist.")
                return
            else:
                raise e

        utc_datetime = datetime.utcnow()
        desired_timezone = pytz.timezone("America/New_York")
        localized_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(
            desired_timezone
        )
        midnight_time = datetime.combine(localized_datetime, time.min)

        result = {
            "headers": ["trackingID", "date", "msgType", "symbol", "price"],
            "data": [],
        }

        keyExpression = Key("date").eq(
            date if date else midnight_time.strftime("%Y-%m-%d")
        )
        filterExpression = Attr("trackingID").gte(0) & Attr("symbol").eq(symbol)

        logger.info(
            f"Querying NASDAQ data for date: {date if date else midnight_time.strftime('%Y-%m-%d')} and symbol: {symbol} at {datetime.now()}"
        )

        async def query_table(exclusive_start_key=None):
            query_args = {
                "IndexName": "fetch_index",
                "KeyConditionExpression": keyExpression,
                "Limit": QUERY_LIMIT,
                "FilterExpression": filterExpression,
            }
            if exclusive_start_key:
                query_args["ExclusiveStartKey"] = exclusive_start_key

            try:
                response = await nasdaq_table.query(**query_args)
                logger.info(
                    f"Queried {len(response['Items'])} items at {datetime.now()}"
                )
                return response
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    logger.error(
                        "The requested resource was not found during the query operation."
                    )
                    return None
                else:
                    raise e

        async def process_items(items):
            logger.info(f"Processing {len(items)} items at {datetime.now()}")
            for item in items:
                try:
                    result["data"].append(
                        [
                            int(item["trackingID"]),
                            item["date"],
                            item["msgType"],
                            item.get("symbol", ""),
                            int(item.get("price", -1)),
                        ]
                    )
                except Exception as e:
                    logger.error(f"Error occurs when saving data: {e}")

        async def query_and_process(exclusive_start_key=None):
            response = await query_table(exclusive_start_key)
            if response is not None:
                await process_items(response["Items"])
                return response.get("LastEvaluatedKey", None)
            return None

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_QUERIES)
        exclusive_start_key = await query_and_process()

        async def query_worker():
            nonlocal exclusive_start_key
            while exclusive_start_key:
                async with semaphore:
                    exclusive_start_key = await query_and_process(exclusive_start_key)

        workers = [query_worker() for _ in range(MAX_CONCURRENT_QUERIES)]
        await asyncio.gather(*workers)

        end_time = datetime.now()
        logger.info(f"End time: {end_time}")
        logger.info(f"Total time taken: {end_time - start_time}")
        logger.info(f"Total items retrieved: {len(result['data'])}")

        return result


async def main():
    date_to_query = "2024-06-18"
    symbols_to_query = ["AAPL", "NVDA", "MSFT"]
    tasks = [get_nasdaq_data(date_to_query, symbol) for symbol in symbols_to_query]
    results = await asyncio.gather(*tasks)

    for symbol, result in zip(symbols_to_query, results):
        print(f"Results for {symbol}:")
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
