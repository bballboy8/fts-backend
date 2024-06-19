import asyncio
import logging
from datetime import datetime
import pytz
import aioboto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculateNanoSec(hour, min, sec, milisec):
    return (hour * 3600 + min * 60 + sec) * 1000 * 1000000 + milisec * 1000000


async def get_nasdaq_data(date=None):
    session = aioboto3.Session()
    async with session.resource("dynamodb") as dynamodb:
        try:
            nasdaq_table = await dynamodb.Table("NASDAQ2")
            # Verify table exists
            await nasdaq_table.load()
            logger.info("Table 'NASDAQ2' loaded successfully.")

            # Verify GSI exists
            table_info = await nasdaq_table.meta.client.describe_table(
                TableName="NASDAQ2"
            )
            indexes = [
                index["IndexName"]
                for index in table_info.get("Table", {}).get(
                    "GlobalSecondaryIndexes", []
                )
            ]
            if "fetch_index" not in indexes:
                logger.error("Index 'fetch_index' does not exist on table 'NASDAQ2'.")
                return
            logger.info("Index 'fetch_index' verified successfully.")

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
        midnight_time = datetime.combine(localized_datetime, datetime.min.time())

        result = {
            "headers": ["trackingID", "date", "msgType", "symbol", "price"],
            "data": [],
        }

        keyExpression = Key("date").eq(
            date if date else midnight_time.strftime("%Y-%m-%d")
        )
        filterExpression = Attr("trackingID").gte(0)

        logger.info(
            f"Querying NASDAQ data for date: {date if date else midnight_time.strftime('%Y-%m-%d')}"
        )

        async def query_table(exclusive_start_key=None):
            query_args = {
                "IndexName": "fetch_index",
                "KeyConditionExpression": keyExpression,
                "Limit": 100000,
                "FilterExpression": filterExpression,
            }
            if exclusive_start_key:
                query_args["ExclusiveStartKey"] = exclusive_start_key

            try:
                response = await nasdaq_table.query(**query_args)
                return response
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    logger.error(
                        "The requested resource was not found during the query operation."
                    )
                    return None
                else:
                    raise e

        response = await query_table()
        if response is None:
            return result

        logger.info(f"Initial query returned {len(response['Items'])} items.")

        async def process_items(items):
            for item in items:
                try:
                    result["data"].append(
                        [
                            int(item["trackingID"]),
                            item["date"],
                            item["msgType"],
                            item["symbol"] if "symbol" in item else "",
                            int(item["price"]) if "price" in item else -1,
                        ]
                    )
                except Exception as e:
                    logger.error(f"Error occurs when saving data: {e}")

        await process_items(response["Items"])

        cnt = len(response["Items"])
        tasks = []

        while "LastEvaluatedKey" in response:
            response = await query_table(response["LastEvaluatedKey"])
            if response is None:
                break
            cnt += len(response["Items"])
            logger.info(
                f"Query returned {len(response['Items'])} items, total count so far: {cnt}"
            )
            tasks.append(process_items(response["Items"]))

        if tasks:
            await asyncio.gather(*tasks)

        logger.info(f"Total items retrieved: {cnt}")
        return result


# if __name__ == "__main__":
#     date_to_query = "2024-06-18"
#     result = asyncio.run(get_nasdaq_data(date_to_query))
#     print(result)
