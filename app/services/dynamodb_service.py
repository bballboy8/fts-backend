import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def get_dynamodb_resource():
    try:
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_REGION')

        return boto3.resource(
            'dynamodb',
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Error: AWS credentials not found or incomplete.")
        return None


def create_dynamodb_tables():
    dynamodb = get_dynamodb_resource()

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
