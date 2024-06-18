from fastapi import HTTPException
from app.services.dynamodb_service import DynamoDBService
from dotenv import load_dotenv
import os
import time
from datetime import timedelta, datetime
from boto3.dynamodb.conditions import Key, Attr
import logging

# Load environment variables
# load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dynamodb = DynamoDBService.get_nasdaq_table()
# The `nasdaq_table` variable is representing a DynamoDB table named 'NASDAQ1'. It is being used to
# perform a scan operation on the table to retrieve items that meet certain filter criteria specified
# in the `scan_kwargs` dictionary. The filter criteria include conditions on the 'id' attribute
# (begins with a specific prefix) and the 'trackingID' attribute (within a specific range of values).
# The scan operation retrieves items from the table based on these criteria and prints them out.
nasdaq_table = dynamodb.Table('NASDAQ2')


def calculateNanoSec(hour, min, sec, milisec):
    return (hour * 3600 + min * 60 + sec) * 1000 * 1000000 + milisec * 1000000


def get_nasdaq_data(date, symbol='AAPL'):
    start_time = time.time()
    result = {
        "headers": ["trackingID", "date", "msgType", "symbol", "price"],
        "data": []
    }
    response = nasdaq_table.query(
        IndexName='fetch_index',
        KeyConditionExpression=Key('date').eq(date),
        Limit = 100000,
        FilterExpression= Attr('symbol').eq(symbol)
    )
    for item in response['Items']:
        result['data'].append([int(item['trackingID']), item['date'], item['msgType'], item['symbol'], int(
            item['price']) if 'price' in item else -1])
    
    cnt = len(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = nasdaq_table.query(
            IndexName='fetch_index',
            KeyConditionExpression=Key('date').eq(date),
            Limit = 100000,
            
            ExclusiveStartKey=response['LastEvaluatedKey'],
            FilterExpression= Attr('symbol').eq(symbol)
        )

        cnt += len(response['Items'])
        for item in response['Items']:
            result['data'].append([int(item['trackingID']), item['date'], item['msgType'], item['symbol'], int(
                item['price']) if 'price' in item else -1])
        print(cnt)
    print(len(result['data']))
    return result

# Print the response
# for item in response['Items']:
#     print(item)
# print(len(response['Items']))
# exit(0)