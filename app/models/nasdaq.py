from fastapi import HTTPException
from app.services.dynamodb_service import DynamoDBService
from dotenv import load_dotenv
import os
import pytz
import time
from datetime import timedelta, datetime
from boto3.dynamodb.conditions import Key, Attr
import logging

# Load environment variables
# load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# The `nasdaq_table` variable is representing a DynamoDB table named 'NASDAQ1'. It is being used to
# perform a scan operation on the table to retrieve items that meet certain filter criteria specified
# in the `scan_kwargs` dictionary. The filter criteria include conditions on the 'id' attribute
# (begins with a specific prefix) and the 'trackingID' attribute (within a specific range of values).
# The scan operation retrieves items from the table based on these criteria and prints them out.

def calculateNanoSec(hour, min, sec, milisec):
    return (hour * 3600 + min * 60 + sec) * 1000 * 1000000 + milisec * 1000000

async def get_nasdaq_data(date=None, timestamp=0, symbol=None):
    dynamodb = await DynamoDBService.get_nasdaq_table()
    nasdaq_table = await dynamodb.Table('NASDAQ2')
    start_time = time.time()
    # Calculate the datetime

    utc_datetime = datetime.now()
    # Set the desired timezone
    desired_timezone = pytz.timezone('America/New_York')
    # Convert the UTC time to the desired timezone
    localized_datetime = utc_datetime.replace(
        tzinfo=pytz.utc).astimezone(desired_timezone)
    midnight_time = datetime.combine(localized_datetime, datetime.min.time())

    result = {
        "headers": ["trackingID", "date", "msgType", "symbol", "price"],
        "data": []
    }
    keyExpression = None
    filterExpression = None
    print(timestamp)
    if date is None:
        keyExpression = Key('date').eq(midnight_time.strftime('%Y-%m-%d'))
    else:
        keyExpression = Key('date').eq(date)

    if timestamp is not None:
        curTimestamp = calculateNanoSec(localized_datetime.hour, localized_datetime.minute,
                                        localized_datetime.second, localized_datetime.microsecond)
        filterExpression = Attr('trackingID').between(timestamp, curTimestamp)
    else:
        filterExpression = Attr('trackingID').gte(0)

    if symbol is not None:
        filterExpression &= Attr('symbol').eq(symbol)

    response = await nasdaq_table.query(
        IndexName='fetch_index',
        KeyConditionExpression=keyExpression,
        Limit=100000,
        FilterExpression=filterExpression
    )
    for item in response['Items']:
        result['data'].append([int(item['trackingID']), item['date'], item['msgType'], item['symbol'], int(
            item['price']) if 'price' in item else -1])

    cnt = len(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = await nasdaq_table.query(
            IndexName='fetch_index',
            KeyConditionExpression=keyExpression,
            Limit=100000,
            FilterExpression=filterExpression,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )

        cnt += len(response['Items'])
        for item in response['Items']:
            try :
                result['data'].append([int(item['trackingID']), item['date'], item['msgType'], item['symbol'] if 'symbol' in item else '', int(
                    item['price']) if 'price' in item else -1])
            except Exception as e:
                logging.error(f"Error occurs when savind data : {e}")
        print(len(result['data']))
    print(len(result['data']))
    return result

# Print the response
# for item in response['Items']:
#     print(item)
# print(len(response['Items']))
# exit(0)
