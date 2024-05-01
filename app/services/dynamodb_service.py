# services/dynamodb_service.py
import boto3
from ..settings import settings

def get_item(item_id: str):
    client = boto3.client(
        'dynamodb',
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
        endpoint_url=settings.dynamodb_endpoint
    )
    response = client.get_item(
        TableName='YourTableName',
        Key={
            'ItemId': {'S': item_id}
        }
    )
    return response['Item']
