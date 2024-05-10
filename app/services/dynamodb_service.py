import boto3
from dotenv import load_dotenv
import os

# create a logging instance
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Load environment variables
load_dotenv()

class DynamoDBService:
    _resource = None

    @classmethod
    def get_resource(cls):
        if cls._resource is None:
            aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            aws_region = os.getenv('AWS_REGION')

            # Add a try-except block to catch exceptions
            try:
                cls._resource = boto3.resource(
                    'dynamodb',
                    region_name=aws_region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )
                logging.info("DynamoDB resource created successfully")  
            except Exception as e:
                logging.error(f"Error creating DynamoDB resource: {str(e)}")
        return cls._resource

# def get_dynamodb_resource():
#     aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
#     aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
#     aws_region = os.getenv('AWS_REGION')

#     return boto3.resource(
#         'dynamodb',
#         region_name=aws_region,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key
#     )


# def create_dynamodb_tables():
#     dynamodb = get_dynamodb_resource()
#     print("Creating tables...")
    
#     # check if the tables already exist
#     existing_tables = dynamodb.tables.all()
#     for table in existing_tables:
#         if table.name == 'Users' or table.name == 'UserSettings':
#             print(f"Table {table.name} already exists")
#             return

#     # Users table
#     user_table = dynamodb.create_table(
#         TableName='Users',
#         KeySchema=[{'AttributeName': 'username', 'KeyType': 'HASH'}],
#         AttributeDefinitions=[{'AttributeName': 'username', 'AttributeType': 'S'}],
#         ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
#     )

#     # User Settings table
#     user_settings_table = dynamodb.create_table(
#         TableName='UserSettings',
#         KeySchema=[{'AttributeName': 'username', 'KeyType': 'HASH'}],
#         AttributeDefinitions=[{'AttributeName': 'username', 'AttributeType': 'S'}],
#         ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
#     )

#     user_table.wait_until_exists()
#     user_settings_table.wait_until_exists()

# if __name__ == "__main__":
#     create_dynamodb_tables()

def create_dynamodb_tables():
    dynamodb = DynamoDBService.get_resource()

    # check if the tables already exist
    existing_tables = list(dynamodb.tables.all())
    if any(table.name in ['Users', 'UserSettings'] for table in existing_tables):
        logging.info("Tables already exist. Skipping table creation.")
        return

    # Users table
    user_table = dynamodb.create_table(
        TableName='Users',
        KeySchema=[{'AttributeName': 'username', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'username', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )

    # User Settings table
    user_settings_table = dynamodb.create_table(
        TableName='UserSettings',
        KeySchema=[{'AttributeName': 'username', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'username', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )

    user_table.wait_until_exists()
    user_settings_table.wait_until_exists()

if __name__ == "__main__":
    create_dynamodb_tables()
