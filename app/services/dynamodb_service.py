import aioboto3.resources
import aioboto3.session
import boto3
from dotenv import load_dotenv
import os
from botocore.config import Config

from application_logger import get_logger

# create a logging instance
logger = get_logger(__name__)


# Load environment variables
load_dotenv()


class DynamoDBService:
    _resource = None
    _nasdaq = None

    @classmethod
    def get_resource(cls):
        if cls._resource is None:
            config = Config(
                connect_timeout=5, read_timeout=5, retries={"max_attempts": 5}
            )
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            aws_region = os.getenv("AWS_REGION")

            # Add a try-except block to catch exceptions
            try:
                cls._resource = boto3.resource(
                    "dynamodb",
                    region_name=aws_region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    config=config,
                )
                logger.info("DynamoDB resource created successfully")
            except Exception as e:
                logger.error(
                    f"Error creating DynamoDB resource: {str(e)}", exc_info=True
                )
        return cls._resource

    @classmethod
    async def get_nasdaq_table(cls):
        config = Config(connect_timeout=5, read_timeout=5, retries={"max_attempts": 5})

        # Add a try-except block to catch exceptions
        try:
            access_key = os.getenv("NASDQA_ACCESS_KEY_ID")
            secret_access = os.getenv("NASDQA_SECRET_ACCESS_KEY")
            region = os.getenv("NASDQA_DEFAULT_REGION")

            session = aioboto3.Session()
            async with session.resource(
                "dynamodb",
                region_name=region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_access,
            ) as resource:
                cls._nasdaq = resource
            logger.info("NASDAQ DynamoDB resource created successfully")
        except Exception as e:
            logger.error(f"Error creating DynamoDB resource: {str(e)}", exc_info=True)
        return cls._nasdaq


def create_dynamodb_tables():
    dynamodb = DynamoDBService.get_resource()

    # check if the tables already exist
    existing_tables = list(dynamodb.tables.all())
    if any(table.name in ["Users", "UserSettings"] for table in existing_tables):
        logger.info("Tables already exist. Skipping table creation.")
        return

    # Users table
    user_table = dynamodb.create_table(
        TableName="Users",
        KeySchema=[{"AttributeName": "email", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "email", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    # User Settings table
    user_settings_table = dynamodb.create_table(
        TableName="UserSettings",
        KeySchema=[{"AttributeName": "email", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "email", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    user_table.wait_until_exists()
    user_settings_table.wait_until_exists()


if __name__ == "__main__":
    create_dynamodb_tables()
